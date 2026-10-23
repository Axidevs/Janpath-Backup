/**
 * server/src/tinyfish/agentClient.js
 *
 * Official TinyFish Agent Client implementing run-sse streaming with polling
 * recovery.
 *
 * - POST https://agent.tinyfish.ai/v1/automation/run-sse with X-API-Key.
 * - Payload: url, goal, output_schema, browser_profile, proxy_config{enabled: false|true, country_code?}, agent_config{max_duration_seconds}.
 * - Never send max_steps or mode.
 * - Events: STARTED, STREAMING_URL, PROGRESS, HEARTBEAT, TF_API_RESULT, COMPLETE.
 * - COMPLETE carries status (COMPLETED | FAILED | CANCELLED) plus result/error.
 * - Disconnect recovery: GET https://agent.tinyfish.ai/v1/runs/{run_id}.
 */

import { tinyfishOutputSchema } from './outputSchema.js';

const DEFAULT_BASE_URL = 'https://agent.tinyfish.ai';
const TERMINAL_SUCCESS = new Set(['COMPLETED', 'SUCCESS']);
const TERMINAL_FAILURE = new Set(['FAILED', 'ERROR', 'CANCELLED']);

function stringifySafely(value) {
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

export function describeTinyFishError(error, fallback = 'Unknown TinyFish error') {
  if (error === null || error === undefined || error === '') {
    return fallback;
  }

  if (typeof error === 'string') {
    return error.trim() || fallback;
  }

  if (typeof error !== 'object') {
    return String(error);
  }

  const parts = [];
  if (error.message) parts.push(String(error.message));
  if (error.code) parts.push(`Code: ${String(error.code)}`);
  if (error.category) parts.push(`Category: ${String(error.category)}`);
  if (error.reason) parts.push(String(error.reason));
  if (error.help_message) parts.push(String(error.help_message));
  if (error.help_url) parts.push(`Help: ${String(error.help_url)}`);
  if (!parts.length && error.error) {
    const nested = describeTinyFishError(error.error, '');
    if (nested) parts.push(nested);
  }

  return parts.length ? parts.join(' | ') : stringifySafely(error);
}

function describeGoalFailure(result) {
  if (!result || typeof result !== 'object' || Array.isArray(result)) {
    return null;
  }
  if (result.status === 'failure' || result.status === 'FAILED') {
    return describeTinyFishError(
      result.reason || result.error || result.message || result,
      'Agent reported a goal-level failure'
    );
  }
  if (result.error) {
    return describeTinyFishError(result.error, 'Agent reported a goal-level failure');
  }
  return null;
}

function formatRunFailure(runData, fallback = 'TinyFish Agent run failed') {
  const errorText = describeTinyFishError(runData?.error, '');
  if (errorText) return `${fallback}: ${errorText}`;

  const goalFailure = describeGoalFailure(runData?.result);
  if (goalFailure) return `${fallback}: ${goalFailure}`;

  const direct = describeTinyFishError(runData?.message, '');
  if (direct) return `${fallback}: ${direct}`;

  return `${fallback}: status=${runData?.status || 'unknown'}`;
}

function makeAbortError(signal) {
  const reason = signal?.reason;
  if (reason instanceof Error) return reason;
  const error = new Error(
    typeof reason === 'string' && reason.trim()
      ? reason
      : 'TinyFish Agent run aborted by client'
  );
  error.name = 'AbortError';
  return error;
}

function abortableSleep(ms, signal) {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(makeAbortError(signal));
      return;
    }

    const timer = setTimeout(() => {
      cleanup();
      resolve();
    }, ms);

    const onAbort = () => {
      clearTimeout(timer);
      cleanup();
      reject(makeAbortError(signal));
    };

    const cleanup = () => {
      if (typeof signal?.removeEventListener === 'function') {
        signal.removeEventListener('abort', onAbort);
      }
    };

    if (typeof signal?.addEventListener === 'function') {
      signal.addEventListener('abort', onAbort, { once: true });
    }
  });
}

async function readTextSafely(response) {
  try {
    if (typeof response?.text === 'function') {
      return (await response.text()) || '';
    }
  } catch {
    // ignore
  }
  return '';
}

function parseSseEvent(line) {
  const trimmed = line.trim();
  if (!trimmed.startsWith('data:')) return null;

  const jsonText = trimmed.slice(5).trim();
  if (!jsonText) return null;

  try {
    const parsed = JSON.parse(jsonText);
    return parsed && typeof parsed === 'object' ? parsed : null;
  } catch {
    return null;
  }
}

export function createAgentClient(options = {}) {
  const apiKey = options.apiKey || '';
  const baseUrl = options.baseUrl || DEFAULT_BASE_URL;
  const fetchFn = options.fetchFn || globalThis.fetch;
  const pollingIntervalMs = options.pollingIntervalMs || 2000;
  const maxPollingAttempts = options.maxPollingAttempts || 30;

  async function emit(onEvent, event) {
    if (typeof onEvent !== 'function') return;
    try {
      await onEvent(event);
    } catch (err) {
      console.error('[AgentClient] onEvent callback error:', err.message);
    }
  }

  async function pollRun(runId, { onEvent, recordedEvents = [], signal } = {}) {
    let lastStatus = null;

    for (let attempt = 1; attempt <= maxPollingAttempts; attempt++) {
      if (signal?.aborted) {
        throw makeAbortError(signal);
      }

      let pollRes;
      try {
        pollRes = await fetchFn(`${baseUrl}/v1/runs/${runId}`, {
          method: 'GET',
          headers: {
            'X-API-Key': apiKey,
            Accept: 'application/json'
          },
          signal
        });
      } catch (err) {
        if (signal?.aborted) throw makeAbortError(signal);
        if (attempt === maxPollingAttempts) {
          throw new Error(
            `TinyFish polling failed after ${maxPollingAttempts} attempts for run ${runId}: ${err.message}`
          );
        }
        await abortableSleep(pollingIntervalMs, signal);
        continue;
      }

      if (!pollRes.ok) {
        const errText = await readTextSafely(pollRes);
        throw new Error(
          `TinyFish run status request failed with status ${pollRes.status}${
            errText ? `: ${errText}` : ''
          }`
        );
      }

      let runData;
      try {
        runData = await pollRes.json();
      } catch {
        throw new Error(`TinyFish run status response for ${runId} was not valid JSON`);
      }

      if (!runData || typeof runData !== 'object') {
        throw new Error(`TinyFish run status response for ${runId} was malformed`);
      }

      const status = typeof runData.status === 'string' ? runData.status : null;
      lastStatus = status || lastStatus;

      await emit(onEvent, {
        type: 'PROGRESS',
        run_id: runId,
        payload: { attempt, status }
      });

      if (status && TERMINAL_SUCCESS.has(status)) {
        const result = runData.result ?? runData.output ?? null;
        if (result === null || result === undefined) {
          throw new Error(`TinyFish Agent run ${runId} reported ${status} without a result`);
        }

        const goalFailure = describeGoalFailure(result);
        if (goalFailure) {
          throw new Error(`TinyFish Agent run failed: ${goalFailure}`);
        }

        await emit(onEvent, {
          type: 'COMPLETE',
          run_id: runId,
          payload: { result, status }
        });

        return { runId, result, events: recordedEvents };
      }

      if (status && TERMINAL_FAILURE.has(status)) {
        throw new Error(formatRunFailure(runData));
      }

      if (attempt < maxPollingAttempts) {
        await abortableSleep(pollingIntervalMs, signal);
      }
    }

    throw new Error(
      `TinyFish polling timed out after ${maxPollingAttempts} attempts for run: ${runId}${
        lastStatus ? ` (last status: ${lastStatus})` : ''
      }`
    );
  }

  return {
    /**
     * Executes an agent run with SSE streaming and polling fallback.
     */
    async run({ url, goal, outputSchema, onEvent, signal }) {
      const payload = {
        url,
        goal,
        output_schema: outputSchema || tinyfishOutputSchema,
        browser_profile: 'lite',
        proxy_config: {
          enabled: false
        },
        agent_config: { max_duration_seconds: 120 }
      };

      const recordedEvents = [];
      let runId = null;

      const handleEvent = async (event) => {
        recordedEvents.push(event);
        if (event.run_id && !runId) {
          runId = event.run_id;
        }
        await emit(onEvent, event);
      };

      let finalResult = null;
      let streamCompleted = false;
      let streamFailure = null;
      let streamInterrupted = null;

      try {
        const response = await fetchFn(`${baseUrl}/v1/automation/run-sse`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-API-Key': apiKey,
            Accept: 'text/event-stream'
          },
          body: JSON.stringify(payload),
          signal
        });

        if (!response.ok) {
          const errText = await readTextSafely(response);
          throw new Error(
            `TinyFish Agent SSE request failed with status ${response.status}${
              errText ? `: ${errText}` : ''
            }`
          );
        }

        const reader = response.body?.getReader ? response.body.getReader() : null;
        if (!reader) {
          throw new Error('TinyFish Agent SSE response contained no readable body');
        }

        const decoder = new TextDecoder();
        let buffer = '';

        const processLine = async (line) => {
          const event = parseSseEvent(line);
          if (!event) return;

          await handleEvent(event);
          if (event.type !== 'COMPLETE') return;

          streamCompleted = true;
          const status =
            typeof event.status === 'string'
              ? event.status
              : typeof event.payload?.status === 'string'
                ? event.payload.status
                : null;
          const result = event.result ?? event.payload?.result ?? null;

          if (status && TERMINAL_FAILURE.has(status)) {
            streamFailure = formatRunFailure({ ...event, status });
            return;
          }

          if (result === null || result === undefined) {
            if (status && TERMINAL_SUCCESS.has(status)) {
              streamFailure = `TinyFish Agent run reported ${status} without a result`;
            }
            return;
          }

          const goalFailure = describeGoalFailure(result);
          if (goalFailure) {
            streamFailure = `TinyFish Agent run failed: ${goalFailure}`;
            return;
          }

          finalResult = result;
        };

        while (true) {
          const { done, value } = await reader.read();
          if (done) break;

          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split(/\r?\n/);
          buffer = lines.pop() || '';

          for (const line of lines) {
            await processLine(line);
          }
        }

        buffer += decoder.decode();
        if (buffer.trim()) {
          for (const line of buffer.split(/\r?\n/)) {
            await processLine(line);
          }
        }
      } catch (err) {
        if (signal?.aborted) throw makeAbortError(signal);
        streamInterrupted = err;
      }

      if (streamFailure) {
        throw new Error(streamFailure);
      }

      if (streamCompleted && finalResult !== null) {
        return { runId: runId || 'unknown', result: finalResult, events: recordedEvents };
      }

      if (runId) {
        if (streamInterrupted) {
          console.warn('[AgentClient] SSE stream interrupted, attempting polling recovery:', streamInterrupted.message);
        }
        return await pollRun(runId, { onEvent: handleEvent, recordedEvents, signal });
      }

      if (streamInterrupted) {
        throw streamInterrupted;
      }

      throw new Error('TinyFish Agent run ended without a valid result or recoverable run_id');
    },

    pollRun,

    /**
     * Cancels an ongoing agent run
     */
    async cancel(runId) {
      if (!runId) return null;
      try {
        const response = await fetchFn(`${baseUrl}/v1/runs/${runId}/cancel`, {
          method: 'POST',
          headers: {
            'X-API-Key': apiKey
          }
        });
        if (!response.ok) {
          const errText = await readTextSafely(response);
          console.warn(
            `[AgentClient] Failed to cancel run ${runId}: HTTP ${response.status}${
              errText ? `: ${errText}` : ''
            }`
          );
          return null;
        }
        return await response.json();
      } catch (err) {
        console.warn(`[AgentClient] Failed to cancel run ${runId}:`, err.message);
        return null;
      }
    }
  };
}
