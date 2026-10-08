import { useEffect, useState } from 'react';
import type { Node } from '@xyflow/react';
import type { DagMeta, NodeField, NodeType, ValidationError } from '../types';
import SqlCheckButton from './SqlCheckButton';

interface Props {
  dagId: string;
  meta: DagMeta;
  onMetaChange: (meta: DagMeta) => void;
  selectedNode: Node | null;
  catalog: NodeType[];
  errors: ValidationError[];
  notice: string;
  onRenameNode: (oldId: string, newId: string) => string | null;
  onParamsChange: (id: string, params: Record<string, unknown>) => void;
  onDeleteNode: (id: string) => void;
}

// 右侧属性面板：未选中节点时编辑 DAG 级属性，选中节点时编辑节点属性
export default function PropertiesPanel(props: Props) {
  const { errors, notice, selectedNode } = props;
  return (
    <aside className="props-panel">
      {notice && <div className="notice-ok">{notice}</div>}
      {errors.length > 0 && (
        <div className="error-box">
          <div className="error-title">校验未通过（{errors.length} 个问题）</div>
          <ul>
            {errors.map((e, i) => (
              <li key={i}>
                {e.node_id ? `[${e.node_id}] ` : ''}
                {e.field ? `${e.field}：` : ''}
                {e.message}
              </li>
            ))}
          </ul>
        </div>
      )}
      {selectedNode ? <NodeSection {...props} node={selectedNode} /> : <DagSection {...props} />}
    </aside>
  );
}

// ---------- DAG 级属性 ----------

function DagSection({ dagId, meta, onMetaChange }: Props) {
  return (
    <div className="props-section">
      <div className="props-title">DAG 属性</div>
      <div className="field">
        <label>DAG ID</label>
        <input type="text" value={dagId} readOnly disabled />
      </div>
      <div className="field">
        <label>描述</label>
        <input
          type="text"
          value={meta.description}
          onChange={e => onMetaChange({ ...meta, description: e.target.value })}
        />
      </div>
      <div className="field">
        <label>调度（cron 表达式）</label>
        <input
          type="text"
          value={meta.schedule}
          placeholder="留空 = 手动触发，如 0 2 * * *"
          onChange={e => onMetaChange({ ...meta, schedule: e.target.value })}
        />
      </div>
      <div className="field">
        <label>重试次数（retries）</label>
        <input
          type="number"
          min={0}
          value={meta.retries}
          onChange={e =>
            onMetaChange({ ...meta, retries: Number.isNaN(e.target.valueAsNumber) ? 0 : e.target.valueAsNumber })
          }
        />
      </div>
      <div className="field">
        <label>重试间隔（秒）</label>
        <input
          type="number"
          min={0}
          value={meta.retryDelay}
          onChange={e =>
            onMetaChange({
              ...meta,
              retryDelay: Number.isNaN(e.target.valueAsNumber) ? 0 : e.target.valueAsNumber,
            })
          }
        />
      </div>
      <div className="field" style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
        <input
          id="alert-on-failure"
          type="checkbox"
          checked={meta.alertOnFailure}
          onChange={e => onMetaChange({ ...meta, alertOnFailure: e.target.checked })}
        />
        <label htmlFor="alert-on-failure" style={{ margin: 0, color: 'var(--text)' }}>
          失败时告警（alert_on_failure）
        </label>
      </div>
    </div>
  );
}

// ---------- 节点属性 ----------

function NodeSection({ node, catalog, onRenameNode, onParamsChange, onDeleteNode }: Props & { node: Node }) {
  const data = node.data as { nodeType?: string; typeLabel?: string; params?: Record<string, unknown> };
  const nodeType = data.nodeType ?? '';
  const typeDef = catalog.find(t => t.type === nodeType);

  const [idDraft, setIdDraft] = useState(node.id);
  const [idError, setIdError] = useState('');

  // 切换选中节点时重置草稿
  useEffect(() => {
    setIdDraft(node.id);
    setIdError('');
  }, [node.id]);

  const applyRename = () => {
    const err = onRenameNode(node.id, idDraft);
    setIdError(err ?? '');
    if (!err) setIdDraft(idDraft.trim());
  };

  const setParam = (name: string, value: unknown) => {
    onParamsChange(node.id, { ...(data.params ?? {}), [name]: value });
  };

  // SQL 校验入口：sql / sql_check 节点且 sql 非空时展示
  const sqlText = typeof data.params?.sql === 'string' ? data.params.sql : '';
  const connRaw = data.params?.conn_id;
  const connId = typeof connRaw === 'string' && connRaw.trim() !== '' ? connRaw : 'warehouse';
  const showSqlCheck = (nodeType === 'sql' || nodeType === 'sql_check') && sqlText.trim() !== '';

  return (
    <div className="props-section">
      <div className="props-title">
        节点属性{typeDef ? `（${typeDef.label}）` : ''}
      </div>
      <div className="field">
        <label>节点 id（即 task_id）</label>
        <div className="node-id-row">
          <input
            type="text"
            value={idDraft}
            onChange={e => setIdDraft(e.target.value)}
            onKeyDown={e => {
              if (e.key === 'Enter') applyRename();
            }}
          />
          <button className="btn" onClick={applyRename}>
            应用
          </button>
        </div>
        <div className="field-tip">小写字母开头，仅含小写字母、数字、下划线；改名会同步更新连线</div>
        {idError && <div className="error-text">{idError}</div>}
      </div>

      {typeDef ? (
        typeDef.fields.map(f => (
          <div className="field" key={f.name}>
            <label>
              {f.label}
              {f.required && <span className="required-star">*</span>}
            </label>
            <FieldInput
              field={f}
              value={(data.params ?? {})[f.name]}
              onChange={v => setParam(f.name, v)}
            />
          </div>
        ))
      ) : (
        <div className="field-tip">未知节点类型「{nodeType}」，无法渲染参数表单</div>
      )}

      {showSqlCheck && <SqlCheckButton sql={sqlText} connId={connId} />}

      <button className="btn delete-node-btn" onClick={() => onDeleteNode(node.id)}>
        删除节点
      </button>
    </div>
  );
}

// 按 field.kind 动态渲染参数输入控件
function FieldInput({
  field,
  value,
  onChange,
}: {
  field: NodeField;
  value: unknown;
  onChange: (v: unknown) => void;
}) {
  switch (field.kind) {
    case 'textarea':
      return <textarea rows={5} value={typeof value === 'string' ? value : value == null ? '' : JSON.stringify(value, null, 2)} onChange={e => onChange(e.target.value)} />;
    case 'select':
      return (
        <select value={(value as string) ?? ''} onChange={e => onChange(e.target.value)}>
          {(field.options ?? []).map(o => (
            <option key={o} value={o}>
              {o}
            </option>
          ))}
        </select>
      );
    case 'number':
      return (
        <input
          type="number"
          value={value === undefined || value === null || value === '' ? '' : Number(value)}
          onChange={e => onChange(e.target.value === '' ? '' : e.target.valueAsNumber)}
        />
      );
    case 'checkbox':
      return (
        <input type="checkbox" checked={Boolean(value)} onChange={e => onChange(e.target.checked)} />
      );
    default:
      return <input type="text" value={(value as string) ?? ''} onChange={e => onChange(e.target.value)} />;
  }
}
