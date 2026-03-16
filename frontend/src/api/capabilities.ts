import { ENDPOINTS } from '@/constants/api';
import { Capability, CreateCompositeCapabilityRequest } from '@/types/action';
import { apiRequest } from '@/lib/api';

export const getCapabilities = async (): Promise<Capability[]> => {
  return apiRequest<Capability[]>(ENDPOINTS.CAPABILITIES.LIST, {
    method: 'GET',
  }).catch(() => []);
};

export const createCompositeCapability = async (
  payload: CreateCompositeCapabilityRequest
): Promise<Capability> => {
  return apiRequest<Capability>(ENDPOINTS.CAPABILITIES.CREATE_COMPOSITE, {
    method: 'POST',
    body: JSON.stringify(payload),
  });
};
