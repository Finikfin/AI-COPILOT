import React from 'react';

const Actions: React.FC = () => {
  return (
    <div className="flex h-full flex-col px-6 py-8">
      <div className="flex items-center justify-between mb-8">
        <div>
          <h1 className="text-2xl font-semibold text-gray-900">Actions</h1>
          <p className="text-gray-500 mt-1">Управление техническими API методами.</p>
        </div>
      </div>
      
      {/* Плейсхолдер для контента загрузки OpenAPI и списка эндпоинтов */}
      <div className="flex-1 rounded-xl bg-white border border-gray-200 shadow-sm flex items-center justify-center">
        <div className="text-center">
          <p className="text-gray-500 mb-2">Здесь будет список Actions.</p>
          <p className="text-sm text-gray-400">Загрузите файл OpenAPI (Swagger), чтобы начать.</p>
        </div>
      </div>
    </div>
  );
};

export default Actions;
