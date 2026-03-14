import { ENDPOINTS } from '@/constants/api';
import { IngestResponse } from '@/types/action';

export const ingestSwagger = async (type: 'file' | 'manual', content: string, filename?: string): Promise<IngestResponse> => {
  const response = await fetch(ENDPOINTS.ACTIONS.INGEST, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      type,
      filename,
      content
    }),
  });

  if (!response.ok) {
    const errorData = await response.json().catch(() => ({}));
    throw new Error(errorData.detail || 'Failed to import Swagger specification');
  }

  return response.json();
};

export const getActions = async () => {
  // Placeholder for when we have a list endpoint
  // const response = await fetch(ENDPOINTS.ACTIONS.LIST);
  // return response.json();
  return [];
};
