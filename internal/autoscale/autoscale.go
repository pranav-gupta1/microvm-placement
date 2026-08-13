// Package autoscale computes how many vmhost pods the fleet should be running.
package autoscale

import (
	"fmt"
	"math"
	"time"
)

// Config parameterises the scaling policy.
type Config struct {
	SlotsPerHost           int
	TargetUtilisation      float64
	MeanTTL                time.Duration
	ProvisionLatency       time.Duration
	ScaleDownStabilization time.Duration
	MinReplicas            int
	MaxReplicas            int
	SlopeWindow            time.Duration
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
	At          time.Time
	ArrivalRate float64
	InflightVMs int
}

// Decision is the scaling verdict, carrying its own reasoning so the dashboard
// and the logs can show why the fleet is the size it is.
type Decision struct {
	Replicas            int
	PredictedRate       float64
	PredictedVMs        float64
	LeadTerm            float64
	HeldByStabilization bool
}

// Autoscaler turns a stream of samples into a replica count.
type Autoscaler struct {
	cfg Config

	history   []Sample
	rateSlope float64

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
func (a *Autoscaler) Decide(s Sample) Decision {
	a.updateSlope(s)

	lead := math.Max(0, a.rateSlope) * a.cfg.ProvisionLatency.Seconds()
	predictedRate := s.ArrivalRate + lead
	predictedVMs := predictedRate * a.cfg.MeanTTL.Seconds()

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
func (a *Autoscaler) updateSlope(s Sample) {
	if n := len(a.history); n > 0 && !s.At.After(a.history[n-1].At) {
		return
	}
	a.history = append(a.history, s)

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
		return a.peak
	}
	a.peak = replicas
	a.peakAt = now
	return replicas
}

// Reset clears all history.
func (a *Autoscaler) Reset() {
	a.history = nil
	a.rateSlope = 0
	a.peak = 0
	a.peakAt = time.Time{}
}
