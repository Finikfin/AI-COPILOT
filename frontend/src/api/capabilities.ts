import { ENDPOINTS } from '@/constants/api';
import { Capability } from '@/types/action';
import { apiRequest } from '@/lib/api';

export const getCapabilities = async (): Promise<Capability[]> => {
  return apiRequest<Capability[]>(ENDPOINTS.CAPABILITIES.LIST, {
    method: 'GET',
  }).catch(() => []);
};
