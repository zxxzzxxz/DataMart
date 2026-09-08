import { useState } from 'react';
import { getToken, setToken } from '../api';

interface Props {
  onSaved: () => void;
  onClose: () => void;
}

// 后端返回 401 时弹出，token 存 localStorage，之后请求自动带上
export default function TokenDialog({ onSaved, onClose }: Props) {
  const [value, setValue] = useState(getToken());

  const save = () => {
    setToken(value.trim());
    onSaved();
  };

  return (
    <div className="modal-mask">
      <div className="modal">
        <h3>访问令牌</h3>
        <p className="muted" style={{ marginTop: 0 }}>
          后端要求认证（401），请输入 Bearer Token：
        </p>
        <input
          type="password"
          value={value}
          autoFocus
          placeholder="token"
          onChange={e => setValue(e.target.value)}
          onKeyDown={e => {
            if (e.key === 'Enter') save();
          }}
        />
        <div className="modal-actions">
          <button className="btn" onClick={onClose}>
            取消
          </button>
          <button className="btn btn-primary" onClick={save}>
            保存
          </button>
        </div>
      </div>
    </div>
  );
}
