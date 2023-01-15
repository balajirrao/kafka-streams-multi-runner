# kafka-streams-multi-runner

This main purpose of this project is to reproduce
a [bug in kafka-streams](https://issues.apache.org/jira/browse/KAFKA-14624) present at least in version 3.3.1 and likely
since earlier.

## Details of the bug

I found that in a production kafka-streams application, during deployments when partitions move from one application
instance to another, state stores returned stale data.

This project can be used to reproduce the bug by running multiple instances of a kafka streams app with the ability to
start and stop instances in an orderly fashion.

Using this I have been able to reproduce the bug in the following setting:

- processor API based app
- number of standby tasks of 1
- cache enabled key-value state store

The interaction between caching and standby tasks is the cause of this bug. When an active task becomes a standby, the
restoration doesn't invalidate the cache, leading to stale values returned by the cached key-value store when the task
becomes active again on the same instance.

## Setup

This project has two sub projects - `coordinator` and `worker`. The worker is a processor-api based kafka-streams app
and the coordinator coordinates the running of multiple instances of the worker.

When the coordinator is run, it creates an input topic and produces certain messages in that topic. The worker stores
some state in a state store and makes some assertions on what it expects the state to be.

The coordinator starts and stops instances of worker according to a "program", a list of integers. An integer in a
program toggles the running state of the instance every 15 seconds. For example, the program "1 2 1" results in the
following sequence of execution

- start instance 1 -> wait 15 seconds -> start instance 2 -> wait 15 seconds -> stop instance 1

## How to run the project

You'll need to install [sbt](https://www.scala-sbt.org/).

- The coordinator expects a worker JAR to be present in a certain location in the project. Run `sbt "worker/assembly"`
  to build the worker JAR.
- The worker expects kafka to be running on `localhost:9085` with plaintext auth.
- Run `sbt "coordinator / run 1 2 3"` to run the program "1 2 3" in the coordinator. This particular program reproduces
  the said bug. If no arguments are passed to the coordinator, it runs a random program.
- The coordinator will output the following
  ```
      appid: 5e23aef6-b9f1-43a1-ba3e-ce7feedd99e6
      [0] starting instance +1
      [1] starting instance +2
      [2] starting instance +3
      [0]!!! BROKEN !!! Expected 58 but found 31 partition: 3
      [1]!!! BROKEN !!! Expected 64 but found 58 partition: 1
  ```

The number in square brackets is the "program-counter". You can find worker logs for this instance
in `{app_id}.{program-counter}.log` 
