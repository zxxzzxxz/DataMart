import { describe, expect, it } from 'vitest';
import { mockRequest } from '../mock';
import type { DesignerDag, NodeType } from '../types';
import catalog from '../generated/node-catalog.json';

describe('canonical preview contracts', () => {
  it('exposes all backend node types and the actual SQL check fields', async () => {
    const nodes = await mockRequest<NodeType[]>('GET', '/node-types');
    expect(nodes.map(node => node.type)).toEqual(catalog.map(node => node.type));
    expect(nodes.find(node => node.type === 'sql_check')?.fields.map(field => field.name)).toContain('pass_value');
    expect(nodes.find(node => node.type === 'sql_check')?.fields.map(field => field.name)).not.toContain('min_rows');
  });

  it('rejects invalid operator parameters from the generated schemas', async () => {
    const result = await mockRequest<{ ok: boolean }>('POST', '/validate', {
      dag_id: 'invalid', nodes: [{ id: 'start', type: 'empty', params: { trigger_rule: 'invalid_rule' } }], edges: [],
    });
    expect(result.ok).toBe(false);
  });

  it('requires the fetched revision and rejects stale editor writes', async () => {
    const model: DesignerDag = {
      dag_id: 'preview_cas', description: 'original', schedule: null, retries: 2,
      retry_delay_seconds: 300, alert_on_failure: true,
      nodes: [{ id: 'start', type: 'empty', params: {}, position: { x: 0, y: 0 } }], edges: [],
    };
    await mockRequest('POST', '/dags', model);
    const original = await mockRequest<DesignerDag & { revision: string }>('GET', '/dags/preview_cas');
    await expect(mockRequest('PUT', '/dags/preview_cas', model)).rejects.toMatchObject({ status: 422 });
    await mockRequest('PUT', '/dags/preview_cas', { ...model, description: 'A', client_revision: original.revision });
    await expect(mockRequest('PUT', '/dags/preview_cas', {
      ...model, description: 'B', client_revision: original.revision,
    })).rejects.toMatchObject({ status: 409 });
  });
});
