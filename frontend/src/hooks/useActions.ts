import { useState, useMemo } from 'react';
import { Action } from '@/types/action';

export const useActions = (initialActions: Action[] = []) => {
  const [actions, setActions] = useState<Action[]>(initialActions);
  const [searchTerm, setSearchTerm] = useState('');

  const filteredActions = useMemo(() => {
    return actions.filter((action) =>
      action.path?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      (action.tags && action.tags[0]?.toLowerCase().includes(searchTerm.toLowerCase())) ||
      action.summary?.toLowerCase().includes(searchTerm.toLowerCase())
    );
  }, [actions, searchTerm]);

  const addActions = (newActions: Action[]) => {
    setActions(prev => [...newActions, ...prev]);
  };

  const removeAction = (id: string) => {
    setActions(prev => prev.filter(a => a.id !== id));
  };

  return {
    actions,
    searchTerm,
    setSearchTerm,
    filteredActions,
    addActions,
    removeAction,
    setActions
  };
};
