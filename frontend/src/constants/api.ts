export const API_BASE_URL = '/api/v1';

export const ENDPOINTS = {
  ACTIONS: {
    INGEST: `${API_BASE_URL}/actions/ingest`,
    LIST: `${API_BASE_URL}/actions/list`,
  },
  AUTH: {
    LOGIN: `${API_BASE_URL}/auth/login`,
    REGISTER: `${API_BASE_URL}/auth/register`,
  },
  PIPELINES: {
    GENERATE: `${API_BASE_URL}/pipelines/generate`,
  }
};
