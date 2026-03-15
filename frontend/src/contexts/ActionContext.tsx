import React, { createContext, useContext, useState, useMemo, useCallback, useEffect } from 'react';
import { Action, Capability } from '@/types/action';
import { getActions } from '@/api/actions';
import { getCapabilities } from '@/api/capabilities';
import { useAuth } from '@/contexts/AuthContext';

interface ActionContextType {
  actions: Action[];
  capabilities: Capability[];
  searchTerm: string;
  setSearchTerm: (term: string) => void;
  filteredActions: Action[];
  filteredCapabilities: Capability[];
  addActions: (newActions: Action[]) => void;
  addCapabilities: (newCapabilities: Capability[]) => void;
  removeAction: (id: string) => void;
  setActions: (actions: Action[]) => void;
  setCapabilities: (capabilities: Capability[]) => void;
}

const ActionContext = createContext<ActionContextType | undefined>(undefined);

export const ActionProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const { isAuthenticated, token, user } = useAuth();
  const [actions, setActions] = useState<Action[]>([]);
  const [capabilities, setCapabilities] = useState<Capability[]>([]);
  const [searchTerm, setSearchTerm] = useState('');

  useEffect(() => {
    if (!isAuthenticated || !token || !user?.id) {
      setActions([]);
      setCapabilities([]);
      return;
    }

    let cancelled = false;

    const loadLibrary = async () => {
      const [nextActions, nextCapabilities] = await Promise.all([
        getActions(),
        getCapabilities(),
      ]);

      if (cancelled) {
        return;
      }

      setActions(nextActions);
      setCapabilities(nextCapabilities);
    };

    void loadLibrary();

    return () => {
      cancelled = true;
    };
  }, [isAuthenticated, token, user?.id]);

  const filteredActions = useMemo(() => {
    return actions.filter((action) =>
      action.path?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      (action.tags && action.tags[0]?.toLowerCase().includes(searchTerm.toLowerCase())) ||
      action.summary?.toLowerCase().includes(searchTerm.toLowerCase())
    );
  }, [actions, searchTerm]);

  const filteredCapabilities = useMemo(() => {
    return capabilities.filter((cap) =>
      cap.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
      (cap.description || '').toLowerCase().includes(searchTerm.toLowerCase())
    );
  }, [capabilities, searchTerm]);

  const addActions = useCallback((newActions: Action[]) => {
    setActions((prev) => {
      const byId = new Map(prev.map((item) => [item.id, item]));
      for (const action of newActions) {
        byId.set(action.id, action);
      }
      return Array.from(byId.values());
    });
  }, []);

  const addCapabilities = useCallback((newCapabilities: Capability[]) => {
    setCapabilities((prev) => {
      const byId = new Map(prev.map((item) => [item.id, item]));
      for (const capability of newCapabilities) {
        byId.set(capability.id, capability);
      }
      return Array.from(byId.values());
    });
  }, []);

  const removeAction = useCallback((id: string) => {
    setActions(prev => prev.filter(a => a.id !== id));
  }, []);

  return (
    <ActionContext.Provider value={{
      actions,
      capabilities,
      searchTerm,
      setSearchTerm,
      filteredActions,
      filteredCapabilities,
      addActions,
      addCapabilities,
      removeAction,
      setActions,
      setCapabilities
    }}>
      {children}
    </ActionContext.Provider>
  );
};

export const useActionsContext = () => {
  const context = useContext(ActionContext);
  if (context === undefined) {
    throw new Error('useActionsContext must be used within an ActionProvider');
  }
  return context;
};
