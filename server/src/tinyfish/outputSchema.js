/**
 * server/src/tinyfish/outputSchema.js
 *
 * Hand-written JSON Schema for TinyFish Agent automation run.
 *
 * CRITICAL TINYFISH API COMPLIANCE:
 * TinyFish strictly enforces allowed JSON Schema keywords:
 * ALLOWED: type, properties, required, items, enum (strings), nullable,
 *          format, anyOf, min/max keywords, propertyOrdering.
 * REJECTED: additionalProperties, const, oneOf.
 * Top-level MUST be an object.
 *
 * DO NOT generate this from Zod or add disallowed keywords.
 */


export const tinyfishOutputSchema = {
  type: 'object',
  required: ['steps', 'facts', 'pages_visited', 'click_path', 'limitations'],
  properties: {
    steps: {
      type: 'array',
      items: {
        type: 'object',
        required: ['step_key', 'status_claimed', 'summary'],
        properties: {
          step_key: {
            type: 'string',
            enum: [
              'eligibility',
              'documents',
              'application_steps',
              'deadline',
              'next_action'
            ]
          },
          status_claimed: {
            type: 'string',
            enum: ['found', 'partially_found', 'not_found']
          },
          summary: {
            type: 'string'
          }
        }
      }
    },
    facts: {
      type: 'array',
      items: {
        type: 'object',
        required: ['kind', 'value', 'url', 'verbatim_quote'],
        properties: {
          kind: {
            type: 'string',
            enum: [
              'age_min',
              'age_max',
              'fee_amount',
              'deadline_date',
              'document'
            ]
          },
          value: {
            type: 'string'
          },
          url: {
            type: 'string'
          },
          verbatim_quote: {
            type: 'string'
          }
        }
      }
    },
    pages_visited: {
      type: 'array',
      items: {
        type: 'string'
      }
    },
    click_path: {
      type: 'array',
      items: {
        type: 'string'
      }
    },
    limitations: {
      type: 'string',
      nullable: true
    }
  }
};
