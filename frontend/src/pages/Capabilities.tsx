import React from 'react';

const Capabilities: React.FC = () => {
  return (
    <div className="flex h-full flex-col px-6 py-8">
      <div className="flex items-center justify-between mb-8">
        <div>
          <h1 className="text-2xl font-semibold text-gray-900">Capabilities</h1>
          <p className="text-gray-500 mt-1">Создание бизнес-навыков из базовых Actions.</p>
        </div>
      </div>
      
      {/* Плейсхолдер для сборки и списка Capabilities */}
      <div className="flex-1 rounded-xl bg-white border border-gray-200 shadow-sm flex items-center justify-center">
        <div className="text-center">
          <p className="text-gray-500 mb-2">У вас пока нет настроенных Capabilities.</p>
          <button className="mt-4 px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 transition-colors">
            Создать новый навык
          </button>
        </div>
      </div>
    </div>
  );
};

export default Capabilities;
