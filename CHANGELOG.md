# Changelog

## [1.1.0](https://github.com/lucasdecamargo/go-kafka-consumer/compare/v1.0.1...v1.1.0) (2026-03-24)


### Features

* align ShutdownTimeout with Kubernetes terminationGracePeriodSeconds ([c88f9c4](https://github.com/lucasdecamargo/go-kafka-consumer/commit/c88f9c45bdeab5f31a640e97e6cf163243c9a512))
* expose kafka_consumer_lag metric for KEDA autoscaling ([b77b88c](https://github.com/lucasdecamargo/go-kafka-consumer/commit/b77b88c9253fe731323f9eee4b4dfc42ccfbe697))
* gate /readyz on partition assignment ([b133ccb](https://github.com/lucasdecamargo/go-kafka-consumer/commit/b133ccbccb46ce7207d28684ae90e0fa4cbab4ba))
* make lag report interval configurable via Config.LagReportInterval ([17ee434](https://github.com/lucasdecamargo/go-kafka-consumer/commit/17ee4344edf752c849995c55e19a8c95a67116d1))


### Bug Fixes

* cast statistics.interval.ms to int to unblock lag metric ([7be99cc](https://github.com/lucasdecamargo/go-kafka-consumer/commit/7be99cc770ee777f15b463d3cfe709fa22e744ce))

## [1.0.1](https://github.com/lucasdecamargo/go-kafka-consumer/compare/v1.0.0...v1.0.1) (2026-03-20)


### Bug Fixes

* rename coverage output files from .out to .txt for Codecov ([94e0dec](https://github.com/lucasdecamargo/go-kafka-consumer/commit/94e0decc43d9e39c79e440fd63b97f920658a2ad))
* resolve golangci-lint v2 violations across codebase ([27c5af4](https://github.com/lucasdecamargo/go-kafka-consumer/commit/27c5af428e46bbaadb0ecc6b0198f0d0081a7daa))

## 1.0.0 (2026-03-20)


### Features

* Releasing first version ([d7a936a](https://github.com/lucasdecamargo/go-kafka-consumer/commit/d7a936a9452b9a19c38c25a114eec8fdb816f2af))
