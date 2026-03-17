export const API_BASE_URL = '/api/v1';

export const ENDPOINTS = {
  ACTIONS: {
    INGEST: `${API_BASE_URL}/actions/ingest`,
    LIST: `${API_BASE_URL}/actions/`,
  },
  CAPABILITIES: {
    LIST: `${API_BASE_URL}/capabilities/`,
    CREATE_COMPOSITE: `${API_BASE_URL}/capabilities/composite`,
  },
  AUTH: {
    LOGIN: `${API_BASE_URL}/auth/login`,
    REGISTER: `${API_BASE_URL}/auth/register`,
  },
  PIPELINES: {
    GENERATE: `${API_BASE_URL}/pipelines/generate`,
    DIALOGS: `${API_BASE_URL}/pipelines/dialogs`,
    DIALOG_HISTORY: (dialogId: string) => `${API_BASE_URL}/pipelines/dialogs/${dialogId}/history`,
    RUN: (pipelineId: string) => `${API_BASE_URL}/pipelines/${pipelineId}/run`,
    GRAPH: (pipelineId: string) => `${API_BASE_URL}/pipelines/${pipelineId}/graph`,
  },
  EXECUTIONS: {
    LIST: `${API_BASE_URL}/executions`,
    GET: (runId: string) => `${API_BASE_URL}/executions/${runId}`,
  }
};
