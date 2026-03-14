import React, { createContext, useContext, useState, useMemo, useCallback } from 'react';
import { Action } from '@/types/action';

interface ActionContextType {
  actions: Action[];
  searchTerm: string;
  setSearchTerm: (term: string) => void;
  filteredActions: Action[];
  addActions: (newActions: Action[]) => void;
  removeAction: (id: string) => void;
  setActions: (actions: Action[]) => void;
}

const ActionContext = createContext<ActionContextType | undefined>(undefined);

export const ActionProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [actions, setActions] = useState<Action[]>([]);
  const [searchTerm, setSearchTerm] = useState('');

  const filteredActions = useMemo(() => {
    return actions.filter((action) =>
      action.path?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      (action.tags && action.tags[0]?.toLowerCase().includes(searchTerm.toLowerCase())) ||
      action.summary?.toLowerCase().includes(searchTerm.toLowerCase())
    );
  }, [actions, searchTerm]);

  const addActions = useCallback((newActions: Action[]) => {
    setActions(prev => [...newActions, ...prev]);
  }, []);

  const removeAction = useCallback((id: string) => {
    setActions(prev => prev.filter(a => a.id !== id));
  }, []);

  return (
    <ActionContext.Provider value={{
      actions,
      searchTerm,
      setSearchTerm,
      filteredActions,
      addActions,
      removeAction,
      setActions
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
