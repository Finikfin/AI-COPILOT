import { ENDPOINTS } from '@/constants/api';
import { Action, IngestResponse } from '@/types/action';
import { apiRequest } from '@/lib/api';

export const ingestSwagger = async (type: 'file' | 'manual', content: string, filename?: string): Promise<IngestResponse> => {
  return apiRequest<IngestResponse>(ENDPOINTS.ACTIONS.INGEST, {
    method: 'POST',
    body: JSON.stringify({
      type,
      filename,
      content
    }),
  });
};

export const getActions = async (): Promise<Action[]> => {
  return apiRequest<Action[]>(ENDPOINTS.ACTIONS.LIST, {
    method: 'GET'
  }).catch(() => []);
};
