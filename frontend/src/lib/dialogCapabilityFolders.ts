export interface DialogCapabilityFolder {
  dialogId: string;
  folderName: string;
  fileNames: string[];
  capabilityIds: string[];
  updatedAt: string;
}

type DialogCapabilityFoldersMap = Record<string, DialogCapabilityFolder>;

const buildStorageKey = (userId?: string) =>
  `pipeline_capability_folders:${userId || 'anonymous'}`;

const buildDialogOrderKey = (userId?: string) =>
  `pipeline_dialog_order:${userId || 'anonymous'}`;

const buildHiddenDialogsKey = (userId?: string) =>
  `pipeline_hidden_dialogs:${userId || 'anonymous'}`;

const dedupeStrings = (items: string[]) =>
  Array.from(new Set(items.map((item) => String(item || '').trim()).filter(Boolean)));

const parseStringArray = (raw: string | null): string[] => {
  if (!raw) {
    return [];
  }

  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) {
      return [];
    }
    return dedupeStrings(parsed.map((value) => String(value || '').trim()));
  } catch {
    return [];
  }
};

const hasFolderContent = (folder?: Partial<DialogCapabilityFolder>): boolean =>
  !!folder && ((folder.fileNames?.length || 0) > 0 || (folder.capabilityIds?.length || 0) > 0);

const parseFoldersMap = (raw: string | null): DialogCapabilityFoldersMap => {
  if (!raw) {
    return {};
  }

  try {
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== 'object') {
      return {};
    }
    return parsed as DialogCapabilityFoldersMap;
  } catch {
    return {};
  }
};

const extractChatIndex = (folderName: string): number | null => {
  const match = /^chat\s*(\d+)$/i.exec(String(folderName || '').trim());
  if (!match) {
    return null;
  }
  const value = Number(match[1]);
  return Number.isFinite(value) && value > 0 ? value : null;
};

const nextChatFolderName = (map: DialogCapabilityFoldersMap): string => {
  let maxIndex = 0;
  for (const folder of Object.values(map)) {
    const idx = extractChatIndex(folder?.folderName || '');
    if (idx && idx > maxIndex) {
      maxIndex = idx;
    }
  }
  return `chat${maxIndex + 1}`;
};

const normalizeFolder = (
  dialogId: string,
  source: Partial<DialogCapabilityFolder> | undefined,
  map: DialogCapabilityFoldersMap
): DialogCapabilityFolder => {
  const rawFolderName = String(source?.folderName || '').trim();
  const folderName = rawFolderName || nextChatFolderName(map);

  return {
    dialogId,
    folderName,
    fileNames: dedupeStrings(source?.fileNames || []),
    capabilityIds: dedupeStrings(source?.capabilityIds || []),
    updatedAt: source?.updatedAt || new Date().toISOString(),
  };
};

const readRawFoldersMap = (userId?: string): DialogCapabilityFoldersMap => {
  if (typeof window === 'undefined') {
    return {};
  }

  const raw = localStorage.getItem(buildStorageKey(userId));
  return parseFoldersMap(raw);
};

const mergeFolderRecords = (
  dialogId: string,
  left: Partial<DialogCapabilityFolder>,
  right: Partial<DialogCapabilityFolder>
): DialogCapabilityFolder => {
  const leftTs = Date.parse(left.updatedAt || '');
  const rightTs = Date.parse(right.updatedAt || '');
  const leftIsNewer = Number.isFinite(leftTs) && (!Number.isFinite(rightTs) || leftTs >= rightTs);
  const preferredFolderName = leftIsNewer ? left.folderName : right.folderName;
  const preferredUpdatedAt = leftIsNewer ? left.updatedAt : right.updatedAt;

  return {
    dialogId,
    folderName: String(preferredFolderName || '').trim(),
    fileNames: dedupeStrings([...(left.fileNames || []), ...(right.fileNames || [])]),
    capabilityIds: dedupeStrings([...(left.capabilityIds || []), ...(right.capabilityIds || [])]),
    updatedAt: preferredUpdatedAt || new Date().toISOString(),
  };
};

const readFoldersMap = (userId?: string): DialogCapabilityFoldersMap => {
  const direct = readRawFoldersMap(userId);
  if (!userId || typeof window === 'undefined') {
    return direct;
  }

  const anonymousMap = readRawFoldersMap(undefined);
  if (Object.keys(anonymousMap).length === 0) {
    return direct;
  }

  const merged: DialogCapabilityFoldersMap = { ...direct };
  let changed = false;

  for (const [dialogId, anonFolder] of Object.entries(anonymousMap)) {
    const existing = merged[dialogId];
    if (!existing) {
      merged[dialogId] = normalizeFolder(dialogId, anonFolder, merged);
      changed = true;
      continue;
    }

    const mergedRecord = normalizeFolder(
      dialogId,
      mergeFolderRecords(dialogId, existing, anonFolder),
      merged
    );

    if (JSON.stringify(existing) !== JSON.stringify(mergedRecord)) {
      merged[dialogId] = mergedRecord;
      changed = true;
    }
  }

  if (changed) {
    writeFoldersMap(userId, merged);
  }

  localStorage.removeItem(buildStorageKey(undefined));
  return merged;
};

const writeFoldersMap = (userId: string | undefined, map: DialogCapabilityFoldersMap) => {
  if (typeof window === 'undefined') {
    return;
  }
  localStorage.setItem(buildStorageKey(userId), JSON.stringify(map));
};

export const getDialogCapabilityFolder = (
  dialogId: string | null | undefined,
  userId?: string
): DialogCapabilityFolder | null => {
  if (!dialogId) {
    return null;
  }
  const map = readFoldersMap(userId);
  const existing = map[dialogId];
  if (!existing) {
    return null;
  }
  const normalized = normalizeFolder(dialogId, existing, map);
  if (
    existing.folderName !== normalized.folderName ||
    existing.updatedAt !== normalized.updatedAt ||
    JSON.stringify(existing.fileNames || []) !== JSON.stringify(normalized.fileNames) ||
    JSON.stringify(existing.capabilityIds || []) !== JSON.stringify(normalized.capabilityIds)
  ) {
    map[dialogId] = normalized;
    writeFoldersMap(userId, map);
  }
  return normalized;
};

export const ensureDialogCapabilityFolder = ({
  dialogId,
  userId,
}: {
  dialogId: string;
  userId?: string;
}): DialogCapabilityFolder => {
  const map = readFoldersMap(userId);
  const normalized = normalizeFolder(dialogId, map[dialogId], map);
  map[dialogId] = normalized;
  writeFoldersMap(userId, map);
  return normalized;
};

export const listDialogCapabilityFolders = (userId?: string): DialogCapabilityFolder[] => {
  const map = readFoldersMap(userId);
  const normalizedMap: DialogCapabilityFoldersMap = { ...map };
  let changed = false;
  const folders = Object.entries(map).map(([dialogId, folder]) => {
    const normalized = normalizeFolder(dialogId, folder, normalizedMap);
    if (
      folder.folderName !== normalized.folderName ||
      folder.updatedAt !== normalized.updatedAt ||
      JSON.stringify(folder.fileNames || []) !== JSON.stringify(normalized.fileNames) ||
      JSON.stringify(folder.capabilityIds || []) !== JSON.stringify(normalized.capabilityIds)
    ) {
      normalizedMap[dialogId] = normalized;
      changed = true;
    }
    return normalized;
  });

  if (changed) {
    writeFoldersMap(userId, normalizedMap);
  }

  return folders
    .filter((folder) => !!folder?.dialogId)
    .sort((a, b) => {
      const aTs = Date.parse(a.updatedAt || "");
      const bTs = Date.parse(b.updatedAt || "");
      if (Number.isNaN(aTs) || Number.isNaN(bTs)) {
        return 0;
      }
      return bTs - aTs;
    });
};

export const upsertDialogCapabilityFolder = ({
  dialogId,
  userId,
  fileName,
  capabilityIds,
}: {
  dialogId: string;
  userId?: string;
  fileName?: string;
  capabilityIds?: string[];
}): DialogCapabilityFolder => {
  const map = readFoldersMap(userId);
  const existing = normalizeFolder(dialogId, map[dialogId], map);

  const next: DialogCapabilityFolder = {
    dialogId,
    folderName: existing.folderName,
    fileNames: dedupeStrings(fileName ? [...existing.fileNames, fileName] : existing.fileNames),
    capabilityIds: dedupeStrings(capabilityIds ? [...existing.capabilityIds, ...capabilityIds] : existing.capabilityIds),
    updatedAt: new Date().toISOString(),
  };

  map[dialogId] = next;
  writeFoldersMap(userId, map);
  return next;
};

export const clearDialogCapabilityFolder = (dialogId: string, userId?: string) => {
  const map = readFoldersMap(userId);
  if (map[dialogId]) {
    delete map[dialogId];
    writeFoldersMap(userId, map);
  }
};

export const setDialogCapabilityFolderName = ({
  dialogId,
  userId,
  folderName,
}: {
  dialogId: string;
  userId?: string;
  folderName: string;
}): DialogCapabilityFolder => {
  const map = readFoldersMap(userId);
  const existing = normalizeFolder(dialogId, map[dialogId], map);
  const normalizedName = String(folderName || '').trim();
  const next: DialogCapabilityFolder = {
    ...existing,
    folderName: normalizedName || existing.folderName,
    updatedAt: new Date().toISOString(),
  };

  map[dialogId] = next;
  writeFoldersMap(userId, map);
  return next;
};

export const getDialogOrder = (userId?: string): string[] => {
  if (typeof window === 'undefined') {
    return [];
  }
  return parseStringArray(localStorage.getItem(buildDialogOrderKey(userId)));
};

export const setDialogOrder = (order: string[], userId?: string): string[] => {
  if (typeof window === 'undefined') {
    return [];
  }

  const normalized = dedupeStrings(order);
  localStorage.setItem(buildDialogOrderKey(userId), JSON.stringify(normalized));
  return normalized;
};

export const syncDialogOrder = (dialogIds: string[], userId?: string): string[] => {
  const incoming = dedupeStrings(dialogIds);
  const existingOrder = getDialogOrder(userId);
  const existingSet = new Set(incoming);
  const kept = existingOrder.filter((id) => existingSet.has(id));
  const missing = incoming.filter((id) => !kept.includes(id));
  return setDialogOrder([...kept, ...missing], userId);
};

export const moveDialogInOrder = ({
  draggedDialogId,
  targetDialogId,
  userId,
}: {
  draggedDialogId: string;
  targetDialogId: string;
  userId?: string;
}): string[] => {
  const order = getDialogOrder(userId);
  if (!order.includes(draggedDialogId)) {
    order.push(draggedDialogId);
  }
  if (!order.includes(targetDialogId)) {
    order.push(targetDialogId);
  }

  const nextOrder = order.filter((id) => id !== draggedDialogId);
  const targetIndex = nextOrder.indexOf(targetDialogId);
  if (targetIndex < 0) {
    nextOrder.push(draggedDialogId);
  } else {
    nextOrder.splice(targetIndex, 0, draggedDialogId);
  }
  return setDialogOrder(nextOrder, userId);
};

export const getHiddenDialogIds = (userId?: string): string[] => {
  if (typeof window === 'undefined') {
    return [];
  }
  return parseStringArray(localStorage.getItem(buildHiddenDialogsKey(userId)));
};

export const hideDialogId = (dialogId: string, userId?: string): string[] => {
  if (typeof window === 'undefined') {
    return [];
  }

  const hidden = dedupeStrings([...getHiddenDialogIds(userId), dialogId]);
  localStorage.setItem(buildHiddenDialogsKey(userId), JSON.stringify(hidden));

  const nextOrder = getDialogOrder(userId).filter((id) => id !== dialogId);
  setDialogOrder(nextOrder, userId);
  return hidden;
};

export const deleteDialogLocally = (dialogId: string, userId?: string): void => {
  clearDialogCapabilityFolder(dialogId, userId);
  hideDialogId(dialogId, userId);
};
