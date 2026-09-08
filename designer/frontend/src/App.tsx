import { useEffect, useState } from 'react';
import { setUnauthorizedHandler } from './api';
import TokenDialog from './components/TokenDialog';
import DagListView from './views/DagListView';
import DesignerView from './views/DesignerView';

type Route = { name: 'list' } | { name: 'canvas'; dagId: string };

export default function App() {
  const [route, setRoute] = useState<Route>({ name: 'list' });
  const [tokenOpen, setTokenOpen] = useState(false);
  // token 保存后 +1，驱动视图重试之前因 401 失败的请求
  const [tokenVersion, setTokenVersion] = useState(0);

  useEffect(() => {
    setUnauthorizedHandler(() => setTokenOpen(true));
  }, []);

  return (
    <div className="app">
      {route.name === 'list' ? (
        <DagListView
          key={`list-${tokenVersion}`}
          onOpen={dagId => setRoute({ name: 'canvas', dagId })}
        />
      ) : (
        <DesignerView
          key={route.dagId}
          dagId={route.dagId}
          tokenVersion={tokenVersion}
          onBack={() => setRoute({ name: 'list' })}
        />
      )}
      {tokenOpen && (
        <TokenDialog
          onClose={() => setTokenOpen(false)}
          onSaved={() => {
            setTokenOpen(false);
            setTokenVersion(v => v + 1);
          }}
        />
      )}
    </div>
  );
}
