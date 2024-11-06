package sync

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	metricsSyncInitCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "gateway",
		Name:      "state_sync_init_total",
		Help:      "The total number of attempts to initialize sync state"},
		[]string{"from", "to"})

	metricsSyncInitErrorCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "gateway",
		Name:      "state_sync_init_error_total",
		Help:      "The total number of attempts to initialize sync state, that resulted in some error"},
		[]string{"from", "to"})

	metricsSyncHead = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "gateway",
		Name:      "state_sync_reached_head",
		Help:      "Whether the sync state reached the head of the 'sync' stream"},
		[]string{"from", "to"})

	metricsSourceDeliverAttempts = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "gateway",
		Name:      "state_sync_source_deliver_attempts_total",
		Help:      "The total number of attempts to deliver some messages by the source"},
		[]string{"from", "to", "source_stream"})

	metricsSourceDeliverAttemptsError = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "gateway",
		Name:      "state_sync_source_deliver_attempts_error_total",
		Help:      "The total number of attempts to deliver some messages by the source, that resulted in some error"},
		[]string{"from", "to", "source_stream"})

	metricsSourceAcceptTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "gateway",
		Name:      "state_sync_source_deliver_messages_accepted_total",
		Help:      "The total number of messages delivered and accepted at the source (then waiting to be sent)"},
		[]string{"from", "to", "source_stream"})

	metricsSourceDeliveredLastSequence = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "gateway",
		Name:      "state_sync_source_delivered_last_sequence",
		Help:      "The last sequence number delivered by the source"},
		[]string{"from", "to", "source_stream"})

	metricsBatchWriteTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "gateway",
		Name:      "state_sync_batch_write_total",
		Help:      "The total number of attempts to create, write and mark dispatched"},
		[]string{"from", "to"})

	metricsBatchWriteErrorTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "gateway",
		Name:      "state_sync_batch_write_error_total",
		Help:      "The total number of attempts to create, write and mark dispatched, that resulted in some error"},
		[]string{"from", "to"})

	metricsHandleIncomingTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "gateway",
		Name:      "state_sync_handle_incoming_total",
		Help:      "The total number of attempts to handle incoming messages"},
		[]string{"from", "to"})

	metricsHandleIncomingErrorTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "gateway",
		Name:      "state_sync_handle_incoming_error_total",
		Help:      "The total number of attempts to handle incoming messages, that resulted in some error"},
		[]string{"from", "to"})

	metricsProcessIncomingTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "gateway",
		Name:      "state_sync_process_incoming_total",
		Help:      "The total number of attempts to process incoming messages"},
		[]string{"from", "to", "source_stream"})

	metricsProcessIncomingErrorTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "gateway",
		Name:      "state_sync_process_incoming_error_total",
		Help:      "The total number of attempts to process incoming messages, that resulted in some error"},
		[]string{"from", "to", "source_stream"})
)
