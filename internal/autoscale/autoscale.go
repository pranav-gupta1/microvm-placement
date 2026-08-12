// Package autoscale computes how many vmhost pods the fleet should be running.
//
// The intelligence lives here rather than in the autoscaling controller. KEDA
// scales a Deployment to match a number; this package decides what that number
// should be and exports it as a Prometheus gauge. Keeping the policy in tested
// Go rather than in a PromQL expression buried in a CRD is the difference
// between something you can reason about and something you tune by guessing.
//
// # The problem with scaling on what already happened
//
// Scaling on current CPU or current occupancy is always late. By the time
// utilisation rises, the traffic that caused it has already arrived, and a new
// pod takes tens of seconds to become useful while a new node takes minutes.
// On a ramp that reaches 1000 requests per second, being 30 seconds late means
// being thousands of requests short.
//
// So the target is not current demand but demand at the moment new capacity
// actually lands:
//
//	predicted_rate = arrival_rate + d(arrival_rate)/dt * provision_latency
//	predicted_vms  = predicted_rate * mean_ttl
//	desired_slots  = max(inflight_vms, predicted_vms) / target_utilisation
//	desired_pods   = ceil(desired_slots / slots_per_pod)
//
// The derivative term is a lead compensator. It buys back exactly the time the
// infrastructure cannot, and it is clamped to be non-negative so that a falling
// arrival rate never causes capacity to be shed faster than the stabilisation
// window allows.
//
// Scale-up is immediate and scale-down is deliberately slow, because the two
// mistakes are not symmetric. Scaling up too eagerly costs money; scaling down
// too eagerly drops requests, and the trough between the assignment's two load
// cycles is precisely where a naive controller scales to zero and then cannot
// recover for the second climb.
package autoscale

import (
	"fmt"
	"math"
	"time"
)

// Config parameterises the scaling policy.
type Config struct {
	// SlotsPerHost is how many microVMs one vmhost pod can run.
	SlotsPerHost int
	// TargetUtilisation is the slot occupancy we aim for, in (0, 1]. Headroom
	// exists because arrivals are Poisson, not smooth.
	TargetUtilisation float64
	// MeanTTL converts an arrival rate into a concurrency via Little's Law.
	MeanTTL time.Duration
	// ProvisionLatency is how long new capacity takes to become useful. This
	// is the horizon the lead term predicts over, so it should be measured,
	// not guessed. See docs/capacity-planning.md.
	ProvisionLatency time.Duration
	// ScaleDownStabilization is how long the fleet must be over-provisioned
	// before capacity is actually removed.
	ScaleDownStabilization time.Duration
	// MinReplicas is the floor, covering the cold start at t=0 when there is
	// no traffic to measure and no history to extrapolate from.
	MinReplicas int
	// MaxReplicas is the ceiling, a blast-radius limit on both cost and the
	// consequences of a bad signal.
	MaxReplicas int
	// SlopeWindow is how much history the derivative is estimated over.
	//
	// This is not a cosmetic smoothing knob, it corrects a real bias. Arrivals
	// are Poisson, so a rate measured over a short interval carries noise of
	// about sqrt(rate/interval). Differencing two such estimates amplifies
	// that noise, and because only a rising rate contributes to the lead term,
	// clamping at zero rectifies symmetric noise into a systematic
	// over-estimate. Measured on the 1000 rps double ramp, differencing
	// consecutive 250 ms samples inflated the fleet from the 79 pods the
	// capacity plan calls for to 198.
	//
	// Estimating the slope by least squares over a window suppresses that
	// noise by roughly the window length to the three-halves power while still
	// tracking a genuine ramp. Defaults to DefaultSlopeWindow.
	SlopeWindow time.Duration
}

// DefaultSlopeWindow is long enough to average away Poisson noise in the
// arrival-rate estimate and short enough to track a real ramp without lag.
const DefaultSlopeWindow = 3 * time.Second

func (c *Config) applyDefaults() {
	if c.SlopeWindow == 0 {
		c.SlopeWindow = DefaultSlopeWindow
	}
}

// Validate reports whether the configuration is usable.
func (c Config) Validate() error {
	if c.SlotsPerHost < 2 {
		return fmt.Errorf("autoscale: SlotsPerHost must be at least 2, got %d", c.SlotsPerHost)
	}
	if c.TargetUtilisation <= 0 || c.TargetUtilisation > 1 {
		return fmt.Errorf("autoscale: TargetUtilisation must be in (0,1], got %v", c.TargetUtilisation)
	}
	if c.MeanTTL <= 0 {
		return fmt.Errorf("autoscale: MeanTTL must be positive, got %s", c.MeanTTL)
	}
	if c.ProvisionLatency < 0 {
		return fmt.Errorf("autoscale: ProvisionLatency must not be negative, got %s", c.ProvisionLatency)
	}
	if c.ScaleDownStabilization < 0 {
		return fmt.Errorf("autoscale: ScaleDownStabilization must not be negative, got %s", c.ScaleDownStabilization)
	}
	if c.MinReplicas < 0 {
		return fmt.Errorf("autoscale: MinReplicas must not be negative, got %d", c.MinReplicas)
	}
	if c.MaxReplicas < c.MinReplicas {
		return fmt.Errorf("autoscale: MaxReplicas %d is below MinReplicas %d", c.MaxReplicas, c.MinReplicas)
	}
	if c.SlopeWindow < 0 {
		return fmt.Errorf("autoscale: SlopeWindow must not be negative, got %s", c.SlopeWindow)
	}
	return nil
}

// Sample is an observation of the system at a point in time.
type Sample struct {
	// At is when the observation was taken.
	At time.Time
	// ArrivalRate is the measured request rate in requests per second.
	ArrivalRate float64
	// InflightVMs is the number of microVMs currently running.
	InflightVMs int
}

// Decision is the scaling verdict, carrying its own reasoning so the dashboard
// and the logs can show why the fleet is the size it is.
type Decision struct {
	// Replicas is the desired vmhost pod count.
	Replicas int
	// PredictedRate is the arrival rate expected once new capacity lands.
	PredictedRate float64
	// PredictedVMs is that rate converted to concurrency by Little's Law.
	PredictedVMs float64
	// LeadTerm is the contribution of the derivative compensator, in requests
	// per second. Positive only while load is climbing.
	LeadTerm float64
	// HeldByStabilization is true when the raw computation wanted to scale
	// down but the stabilisation window vetoed it.
	HeldByStabilization bool
}

// Autoscaler turns a stream of samples into a replica count.
//
// It is not safe for concurrent use; the metrics exporter calls it from one
// place on a fixed interval.
type Autoscaler struct {
	cfg Config

	// history holds recent samples inside the slope window, oldest first.
	history   []Sample
	rateSlope float64

	// peak is the highest replica count decided within the stabilisation
	// window, and the floor for scale-down.
	peak   int
	peakAt time.Time
}

// New returns an Autoscaler.
func New(cfg Config) (*Autoscaler, error) {
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Autoscaler{cfg: cfg}, nil
}

// Decide computes the desired replica count for a sample.
//
// Samples must be supplied in non-decreasing time order. An out-of-order or
// zero-interval sample reuses the previous slope rather than dividing by zero.
func (a *Autoscaler) Decide(s Sample) Decision {
	a.updateSlope(s)

	// Extrapolate the arrival rate over the provisioning horizon. Only a
	// rising rate contributes: a falling one must not pull capacity out from
	// under work that is still arriving.
	lead := math.Max(0, a.rateSlope) * a.cfg.ProvisionLatency.Seconds()
	predictedRate := s.ArrivalRate + lead
	predictedVMs := predictedRate * a.cfg.MeanTTL.Seconds()

	// Inflight is measured truth and acts as a floor: whatever the model
	// predicts, capacity already in use must be covered.
	required := math.Max(predictedVMs, float64(s.InflightVMs))
	slots := required / a.cfg.TargetUtilisation
	replicas := int(math.Ceil(slots / float64(a.cfg.SlotsPerHost)))

	replicas = a.clamp(replicas)
	raw := replicas

	replicas = a.applyStabilization(s.At, replicas)

	return Decision{
		Replicas:            replicas,
		PredictedRate:       predictedRate,
		PredictedVMs:        predictedVMs,
		LeadTerm:            lead,
		HeldByStabilization: replicas > raw,
	}
}

// updateSlope re-estimates d(arrival_rate)/dt by ordinary least squares over
// every sample inside the slope window.
//
// Regression rather than differencing because the arrival rate is a noisy
// measurement of a Poisson process, and the lead term's clamp at zero turns
// noise in the derivative into a systematic over-provision. See the commentary
// on Config.SlopeWindow.
func (a *Autoscaler) updateSlope(s Sample) {
	if n := len(a.history); n > 0 && !s.At.After(a.history[n-1].At) {
		// Duplicate or out-of-order sample. Keep the current estimate rather
		// than corrupting the regression with a non-monotonic time axis.
		return
	}
	a.history = append(a.history, s)

	// Drop anything that has fallen out of the window, always keeping at least
	// two points so a slope remains computable.
	cutoff := s.At.Add(-a.cfg.SlopeWindow)
	drop := 0
	for drop < len(a.history)-2 && a.history[drop].At.Before(cutoff) {
		drop++
	}
	a.history = a.history[drop:]

	if len(a.history) < 2 {
		a.rateSlope = 0
		return
	}

	// Least squares slope, with time measured relative to the first retained
	// sample so the numbers stay small and well conditioned.
	base := a.history[0].At
	var sumT, sumR, sumTT, sumTR float64
	n := float64(len(a.history))
	for _, h := range a.history {
		t := h.At.Sub(base).Seconds()
		sumT += t
		sumR += h.ArrivalRate
		sumTT += t * t
		sumTR += t * h.ArrivalRate
	}
	denom := n*sumTT - sumT*sumT
	if denom == 0 {
		// Every retained sample shares a timestamp, so no slope is defined.
		a.rateSlope = 0
		return
	}
	a.rateSlope = (n*sumTR - sumT*sumR) / denom
}

func (a *Autoscaler) clamp(replicas int) int {
	if replicas < a.cfg.MinReplicas {
		replicas = a.cfg.MinReplicas
	}
	if replicas > a.cfg.MaxReplicas {
		replicas = a.cfg.MaxReplicas
	}
	return replicas
}

// applyStabilization lets the fleet grow instantly but shrink only after the
// stabilisation window has passed without a higher demand.
func (a *Autoscaler) applyStabilization(now time.Time, replicas int) int {
	if replicas >= a.peak {
		a.peak = replicas
		a.peakAt = now
		return replicas
	}
	if a.cfg.ScaleDownStabilization == 0 {
		a.peak = replicas
		a.peakAt = now
		return replicas
	}
	if now.Sub(a.peakAt) < a.cfg.ScaleDownStabilization {
		// Still inside the window, so hold the higher count.
		return a.peak
	}
	// The window has elapsed with sustained lower demand. Accept the drop and
	// restart the window from here.
	a.peak = replicas
	a.peakAt = now
	return replicas
}

// Reset clears all history. Used by tests and on leader election changeover.
func (a *Autoscaler) Reset() {
	a.history = nil
	a.rateSlope = 0
	a.peak = 0
	a.peakAt = time.Time{}
}
