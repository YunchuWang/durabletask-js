// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

/**
 * Opt-in, bounded Azure test. Uses a dedicated task hub; never injects work items.
 * DTS_HISTORY_STREAMING_E2E=1 and DTS_CONNECTION_STRING are required.
 * DTS_HISTORY_STREAMING_ROUNDS defaults to 1 and is capped at 8.
 *
 * Each activity input/output is below DTS's 1 MiB payload limit. The accumulated
 * history is larger, but the service decides whether to stream it. This test
 * fails if it does not observe genuine worker history streaming.
 */
import { randomBytes, randomUUID } from "crypto";
import * as grpc from "@grpc/grpc-js";
import { ActivityContext, NoOpLogger, OrchestrationContext, OrchestrationStatus } from "@microsoft/durabletask-js";
import {
  DurableTaskAzureManagedClientBuilder,
  DurableTaskAzureManagedWorkerBuilder,
} from "@microsoft/durabletask-js-azuremanaged";
import * as pb from "../../packages/durabletask-js/src/proto/orchestrator_service_pb";
import * as stubs from "../../packages/durabletask-js/src/proto/orchestrator_service_grpc_pb";

const describeAzure = process.env.DTS_HISTORY_STREAMING_E2E === "1" ? describe : describe.skip;

describeAzure("Azure worker history streaming negotiation", () => {
  it("hydrates service-selected history and completes a replayed durable outcome", async () => {
    const connectionString = process.env.DTS_CONNECTION_STRING;
    if (!connectionString || !/Endpoint=https:\/\//i.test(connectionString)) {
      throw new Error("Set DTS_CONNECTION_STRING to a dedicated Azure HTTPS task hub.");
    }
    const rounds = Number(process.env.DTS_HISTORY_STREAMING_ROUNDS ?? 1);
    if (!Number.isInteger(rounds) || rounds < 1 || rounds > 8) {
      throw new Error("DTS_HISTORY_STREAMING_ROUNDS must be an integer from 1 to 8.");
    }

    const instanceId = `js-history-streaming-${randomUUID()}`;
    const workItems: { instanceId: string; executionId?: string; streaming: boolean; past: number; new: number }[] = [];
    const histories: {
      instanceId: string;
      executionId?: string;
      forWorkItemProcessing: boolean;
      chunks: number;
      events: number;
      bytes: number;
      ended: boolean;
      error?: string;
    }[] = [];
    const getWorkItems = stubs.TaskHubSidecarServiceClient.prototype.getWorkItems;
    const streamHistory = stubs.TaskHubSidecarServiceClient.prototype.streamInstanceHistory;
    jest.spyOn(stubs.TaskHubSidecarServiceClient.prototype, "getWorkItems").mockImplementation(function (
      this: stubs.TaskHubSidecarServiceClient,
      request: pb.GetWorkItemsRequest,
      metadata?: grpc.Metadata,
      options?: Partial<grpc.CallOptions>,
    ) {
      const stream = getWorkItems.call(this, request, metadata, options);
      stream.on("data", (item: pb.WorkItem) => {
        const req = item.getOrchestratorrequest();
        if (req?.getInstanceid() === instanceId) {
          workItems.push({
            instanceId: req.getInstanceid(),
            executionId: req.getExecutionid()?.getValue(),
            streaming: req.getRequireshistorystreaming(),
            past: req.getPasteventsList().length,
            new: req.getNeweventsList().length,
          });
        }
      });
      return stream;
    });
    jest.spyOn(stubs.TaskHubSidecarServiceClient.prototype, "streamInstanceHistory").mockImplementation(function (
      this: stubs.TaskHubSidecarServiceClient,
      request: pb.StreamInstanceHistoryRequest,
      metadata?: grpc.Metadata,
      options?: Partial<grpc.CallOptions>,
    ) {
      const stream = streamHistory.call(this, request, metadata, options);
      const observation: (typeof histories)[number] = {
        instanceId: request.getInstanceid(),
        executionId: request.getExecutionid()?.getValue(),
        forWorkItemProcessing: request.getForworkitemprocessing(),
        chunks: 0,
        events: 0,
        bytes: 0,
        ended: false,
      };
      histories.push(observation);
      const onData = (historyChunk: pb.HistoryChunk) => {
        observation.chunks++;
        observation.events += historyChunk.getEventsList().length;
        for (const event of historyChunk.getEventsList()) {
          observation.bytes += event.serializeBinary().length;
        }
      };
      stream.on("data", onData);
      stream.once("end", () => {
        observation.ended = true;
        stream.removeListener("data", onData);
      });
      stream.once("error", (error: grpc.ServiceError) => {
        observation.error = `${error.code}: ${error.details}`;
        stream.removeListener("data", onData);
      });
      return stream;
    });

    const client = new DurableTaskAzureManagedClientBuilder()
      .connectionString(connectionString)
      .logger(new NoOpLogger())
      .build();
    const worker = new DurableTaskAzureManagedWorkerBuilder()
      .connectionString(connectionString)
      .logger(new NoOpLogger())
      .build();
    type Echo = { payload: string; index: number };
    const echo = async function historyStreamingEcho(_ctx: ActivityContext, input: Echo): Promise<Echo> {
      return input;
    };
    const orchestrator = async function* boundedStreamingHistory(
      ctx: OrchestrationContext,
      input: { payload: string; rounds: number },
    ): AsyncGenerator {
      const indices: number[] = [];
      for (let index = 0; index < input.rounds; index++) {
        const result: Echo = yield ctx.callActivity(echo, { payload: input.payload, index });
        if (result.payload !== input.payload || result.index !== index) {
          throw new Error("Replayed activity payload or ordering was corrupted.");
        }
        indices.push(result.index);
      }
      // Ensure the last activity result is persisted into past history for another replay.
      yield ctx.createTimer(new Date(ctx.currentUtcDateTime.getTime() + 1000));
      return { indices, payloadBytes: input.payload.length };
    };
    worker.addActivity(echo);
    worker.addOrchestrator(orchestrator);
    const payload = randomBytes(576 * 1024).toString("base64");
    let started = false;
    let scheduled = false;
    let completed = false;
    let outcome: unknown;
    try {
      await worker.start();
      started = true;
      await client.scheduleNewOrchestration(orchestrator, { payload, rounds }, { instanceId });
      scheduled = true;
      const state = await client.waitForOrchestrationCompletion(instanceId, undefined, 90);
      completed = state?.runtimeStatus === OrchestrationStatus.COMPLETED;
      outcome = { status: state?.runtimeStatus, output: state?.serializedOutput, failure: state?.failureDetails };
      expect(completed).toBe(true);
      expect(JSON.parse(state!.serializedOutput!)).toEqual({
        indices: Array.from({ length: rounds }, (_, i) => i),
        payloadBytes: payload.length,
      });
      const streamedWorkItems = workItems.filter((item) => item.streaming);
      if (streamedWorkItems.length === 0) {
        throw new Error(`Azure did not request history streaming within the bounded ${rounds}-activity workload.`);
      }
      expect(histories).toHaveLength(streamedWorkItems.length);
      for (const history of histories) {
        expect(history.instanceId).toBe(instanceId);
        expect(history.executionId).toBeTruthy();
        expect(streamedWorkItems.some((item) => item.executionId === history.executionId)).toBe(true);
        expect(history.forWorkItemProcessing).toBe(true);
        expect(history.ended).toBe(true);
        expect(history.error).toBeUndefined();
        expect(history.events).toBeGreaterThan(0);
      }
      expect(histories.some((history) => history.chunks > 1)).toBe(true);
    } finally {
      console.log(
        "HISTORY_STREAMING_EVIDENCE",
        JSON.stringify({ instanceId, rounds, payloadBytes: payload.length, workItems, histories, outcome }),
      );
      try {
        if (scheduled) {
          if (!completed) {
            await client.terminateOrchestration(instanceId, "history-streaming-test-cleanup");
            await client.waitForOrchestrationCompletion(instanceId, undefined, 20);
          }
          await client.purgeOrchestration(instanceId);
        }
      } finally {
        if (started) await worker.stop();
        await client.stop();
        jest.restoreAllMocks();
      }
    }
  }, 150000);
});
