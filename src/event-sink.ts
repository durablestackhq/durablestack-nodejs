import type { DurableStackEvent, DurableStackEventSink } from "./types.js";

export class NoOpDurableStackEventSink implements DurableStackEventSink {
  async publish(_event: DurableStackEvent): Promise<void> {
    return Promise.resolve();
  }
}

export class InMemoryEventSink implements DurableStackEventSink {
  public readonly events: DurableStackEvent[] = [];

  async publish(event: DurableStackEvent): Promise<void> {
    this.events.push(event);
  }
}
