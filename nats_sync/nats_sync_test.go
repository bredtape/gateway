package nats_sync

// test scenarios

// low level sync service up and running:
// spin up 2 nats servers to represent deployment A and B
// start nats sync on each with file exchange for inter-communication
// create nats subscription stream on each
// publish subscription to sync the subscription stream itself from A to B

// with sync of request/reply stream:
// create a request and reply stream on each deployment for the NatsSyncService
// then
// * publish a Subscribe request to sync the request and reply for NatsSyncService
// * request stream info of A from A
// * request stream info of B from A

// with http request over nats:
// start http over nats service on each deployment
// publish a Subscribe request to sync the http over nats service
// then
// * issue a http request of some http endpoint at B from A (could simply be Prometheus metrics from the NatsSyncService itself)
// * issue a http request for A from A
