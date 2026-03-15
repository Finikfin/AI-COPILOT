import React, { createContext, useContext, useState, useCallback } from 'react';
import { PipelineData } from '@/types/pipeline';

interface PipelineContextType {
  currentPipeline: PipelineData | null;
  setPipeline: (pipeline: PipelineData | null) => void;
}

const PipelineContext = createContext<PipelineContextType | undefined>(undefined);

export const PipelineProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [currentPipeline, setCurrentPipeline] = useState<PipelineData | null>(null);

  const setPipeline = useCallback((pipeline: PipelineData | null) => {
    setCurrentPipeline(pipeline);
  }, []);

  return (
    <PipelineContext.Provider value={{ currentPipeline, setPipeline }}>
      {children}
    </PipelineContext.Provider>
  );
};

export const usePipelineContext = () => {
  const context = useContext(PipelineContext);
  if (context === undefined) {
    throw new Error('usePipelineContext must be used within a PipelineProvider');
  }
  return context;
};
