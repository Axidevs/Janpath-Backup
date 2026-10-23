/**
 * server/src/services/runAudit.js
 *
 * Full Audit Orchestration Service for JanPath.
 *
 * ORCHESTRATION PIPELINE:
 * 1. Validate start URL with security guard.
 * 2. Ensure audit is recorded as RUNNING and holds limiter slot.
 * 3. Run TinyFish Agent via agentClient, streaming events to store.
 * 4. Store raw events and rawResult BEFORE any processing.
 * 5. Zod-validate rawResult: if invalid -> set EXPLORATION_FAILED, no silent retry.
 * 6. Collect all cited URLs from facts and pages_visited.
 * 7. Independently fetch cited pages with ttl: 0 via fetchClient.
 * 8. Store retrieved pages with cryptographic SHA-256 hashes.
 * 9. Independently verify every quote with verifier engine.
 * 10. Derive finding statuses (VERIFIED_FOUND, UNVERIFIED_CLAIM, NOT_FOUND_IN_SCOPE, UNCONFIRMED).
 * 11. Run deterministic rules (missing info, conflicts, scattered requirements, past deadlines, broken next steps).
 * 12. Save final audit status (COMPLETED or PARTIAL) and release limiter slot.
 */

import {
  AuditStatus,
  EvidenceStatus,
  FindingStatus,
  StepKey
} from '../../../shared/constants.js';
import { AgentResultSchema } from '../../../shared/schemas.js';
import { validateUrl, isHostAllowed } from '../lib/urlGuard.js';
import { verifyEvidence } from '../engine/verifier.js';
import { evaluateMissingInformation } from '../engine/rules/missingInformationRule.js';
import { runAllRules } from '../engine/rules/index.js';
import { buildAgentGoal } from '../tinyfish/goal.js';

export function createRunAuditService(dependencies = {}) {
  const { store, limiter, agentClient, fetchClient, config } = dependencies;

  return {
    /**
     * Executes the end-to-end audit lifecycle for an audit ID.
     *
     * @param {string} auditId
     * @param {Object} [options]
     * @param {Date|Object} [options.referenceDate] - Injectable date for testing
     * @returns {Promise<Object>} Completed audit document
     */
    async execute(auditId, options = {}) {
      const timings = { stages: [], totalStart: Date.now() };
      const mark = (name, start) => {
        const end = Date.now();
        timings.stages.push({ name, durationMs: end - start, start, end });
        return end;
      };
      const t0 = Date.now();

      const audit = await store.getAudit(auditId);
      if (!audit) {
        throw new Error(`Audit "${auditId}" not found in store`);
      }
      const t1 = mark('load_audit', t0);

      try {
        // 1. Security check on startUrl
        const tGuardStart = Date.now();
        const guard = validateUrl(audit.startUrl, config?.fixtureHost);
        mark('security_guard', tGuardStart);
        if (!guard.allowed) {
          await store.updateAudit(auditId, {
            status: AuditStatus.EXPLORATION_FAILED,
            failureReason: `Security guard rejected URL: ${guard.reason}`,
            completedAt: new Date().toISOString()
          });
          timings.totalMs = Date.now() - timings.totalStart;
          console.log(`[runAudit:timing] ${auditId} ${JSON.stringify(timings)}`);
          return await store.getAudit(auditId);
        }

        // 2. Build goal prompt
        const tGoalStart = Date.now();
        const goalPrompt = buildAgentGoal({
          startUrl: audit.startUrl,
          customGoal: audit.goal
        });
        mark('build_goal', tGoalStart);

        // 3. Run TinyFish Agent with event streaming
        let agentExecution;
        const tAgentStart = Date.now();
        try {
          agentExecution = await agentClient.run({
            url: audit.startUrl,
            goal: goalPrompt,
            onEvent: async (event) => {
              await store.createRunEvent({
                auditId,
                type: event.type,
                timestamp: event.timestamp || new Date().toISOString(),
                payload: event.payload || event,
                step: event.step
              });
            }
          });
        } catch (agentErr) {
          mark('agent_exploration_failed', tAgentStart);
          console.error(`[runAudit] Agent execution failed for ${auditId}:`, agentErr.message);
          await store.updateAudit(auditId, {
            status: AuditStatus.EXPLORATION_FAILED,
            failureReason: `Agent execution failed: ${agentErr.message}`,
            completedAt: new Date().toISOString()
          });
          timings.totalMs = Date.now() - timings.totalStart;
          console.log(`[runAudit:timing] ${auditId} ${JSON.stringify(timings)}`);
          return await store.getAudit(auditId);
        }
        mark('agent_exploration', tAgentStart);

        const rawResult = agentExecution.result;

        // 4. Store rawResult BEFORE any validation or processing
        const tStoreStart = Date.now();
        await store.updateAudit(auditId, { rawResult });
        mark('store_raw_result', tStoreStart);

        // 5. Zod schema validation (Strict failure on invalid output - no silent retry)
        const tValidateStart = Date.now();
        const parseCheck = AgentResultSchema.safeParse(rawResult);
        if (!parseCheck.success) {
          mark('schema_validation_failed', tValidateStart);
          const issuesList = parseCheck.error.issues || parseCheck.error.errors || [];
          const schemaErrorMsg = issuesList
            .map((e) => `${e.path?.join('.') || 'field'}: ${e.message}`)
            .join('; ') || 'Invalid agent output structure';
          await store.updateAudit(auditId, {
            status: AuditStatus.EXPLORATION_FAILED,
            failureReason: `Agent output schema validation failed: ${schemaErrorMsg}`,
            completedAt: new Date().toISOString()
          });
          timings.totalMs = Date.now() - timings.totalStart;
          console.log(`[runAudit:timing] ${auditId} ${JSON.stringify(timings)}`);
          return await store.getAudit(auditId);
        }
        const validatedResult = parseCheck.data;
        mark('schema_validation', tValidateStart);

        // 6. Collect all cited URLs to fetch independently
        const tCollectStart = Date.now();
        const citedUrls = new Set();
        for (const fact of validatedResult.facts || []) {
          if (fact.url) citedUrls.add(fact.url);
        }
        for (const pageUrl of validatedResult.pages_visited || []) {
          if (pageUrl) citedUrls.add(pageUrl);
        }
        citedUrls.add(audit.startUrl);
        mark('collect_cited_urls', tCollectStart);

        // 7. Batch-fetch cited pages via Fetch API with ttl: 0
        const tFetchStart = Date.now();
        const fetchedPagesMap = await fetchClient.fetchUrls(Array.from(citedUrls));
        mark('fetch_evidence', tFetchStart);

        // 8. Store retrieved pages with cryptographic SHA-256 hashes
        const tStorePagesStart = Date.now();
        const pageRecords = [];
        for (const [url, pageData] of fetchedPagesMap.entries()) {
          const record = {
            url,
            hash: pageData.hash,
            charCount: pageData.charCount,
            status: pageData.status,
            retrievedAt: new Date().toISOString()
          };
          pageRecords.push(record);

          // Save full page text to pages collection
          await store.savePage({
            auditId,
            url,
            hash: pageData.hash,
            charCount: pageData.charCount,
            text: pageData.text,
            status: pageData.status
          });
        }
        mark('store_pages', tStorePagesStart);

        // 9. Independently verify evidence for each claimed fact
        const tVerifyStart = Date.now();
        const findings = [];
        const verifiedFactsForRules = [];

        for (let i = 0; i < validatedResult.facts.length; i++) {
          const fact = validatedResult.facts[i];
          const pageResult = fetchedPagesMap.get(fact.url);

          const verification = verifyEvidence({
            quote: fact.verbatim_quote,
            retrievedPage: pageResult,
            fetchError: pageResult?.error,
            requestedUrl: fact.url,
            options: {
              isHostAllowed: (h) => isHostAllowed(h, config?.fixtureHost)
            }
          });

          // 10. Derive finding status
          const isMatched = verification.status === EvidenceStatus.SOURCE_MATCHED;
          const findingStatus = isMatched
            ? FindingStatus.VERIFIED_FOUND
            : FindingStatus.UNVERIFIED_CLAIM;

          const finding = {
            id: `finding-${i + 1}`,
            stepKey: fact.kind === 'document' ? StepKey.DOCUMENTS : StepKey.ELIGIBILITY,
            kind: fact.kind,
            value: fact.value,
            status: findingStatus,
            sourceUrl: fact.url,
            evidence: {
              url: fact.url,
              quote: fact.verbatim_quote,
              status: verification.status,
              reason: verification.reason,
              matchedContext: verification.matchedContext,
              matchedSnippet: verification.matchedSnippet,
              finalUrl: verification.finalUrl,
              verifiedAt: new Date().toISOString()
            }
          };

          findings.push(finding);
          verifiedFactsForRules.push({
            ...fact,
            id: finding.id,
            evidence: finding.evidence
          });
        }

        // Derive step findings for steps reported not_found
        const allPagesText = Array.from(fetchedPagesMap.values()).map((p) => p.text);

        for (const step of validatedResult.steps || []) {
          if (step.status_claimed === 'not_found') {
            const missingEval = evaluateMissingInformation({
              stepResult: step,
              auditStatus: AuditStatus.COMPLETED,
              pagesText: allPagesText
            });

            findings.push({
              id: `finding-step-${step.step_key}`,
              stepKey: step.step_key,
              kind: 'step_status',
              value: step.status_claimed,
              status: missingEval?.status || FindingStatus.UNCONFIRMED,
              notes: step.summary
            });
          }
        }

        // 11. Run deterministic rules engine
        const tRulesStart = Date.now();
        const issues = runAllRules({
          audit: { ...audit, status: AuditStatus.COMPLETED },
          facts: verifiedFactsForRules,
          steps: validatedResult.steps,
          pagesText: allPagesText,
          pages: pageRecords,
          referenceDate: options.referenceDate
        });
        mark('rules_engine', tRulesStart);

        // 12. Determine final audit status
        const anyRetrievalFailures = Array.from(fetchedPagesMap.values()).some(
          (p) => Boolean(p.error)
        );
        const finalStatus = anyRetrievalFailures
          ? AuditStatus.PARTIAL
          : AuditStatus.COMPLETED;

        // 13. Save updated audit
        const tSaveStart = Date.now();
        const updatedAudit = await store.updateAudit(auditId, {
          status: finalStatus,
          findings,
          issues,
          pages: pageRecords,
          completedAt: new Date().toISOString()
        });
        mark('save_final', tSaveStart);
        mark('verify_evidence_total', tVerifyStart);

        timings.totalMs = Date.now() - timings.totalStart;
        console.log(`[runAudit:timing] ${auditId} ${JSON.stringify(timings)}`);

        return updatedAudit;
      } finally {
        // ALWAYS release the limiter slot upon termination or completion
        limiter.release(auditId);
      }
    }
  };
}
