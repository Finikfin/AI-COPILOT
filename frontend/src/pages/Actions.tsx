import React, { useState } from 'react';
import {
  Plus,
  Search,
  FileJson,
  MoreHorizontal,
  Trash2,
  ExternalLink,
  ChevronRight
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { Badge } from '@/components/ui/badge';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { SwaggerImportModal } from '@/components/shared/SwaggerImportModal';
import { ImportResultsModal } from '@/components/shared/ImportResultsModal';
import { toast } from 'sonner';
import { Action } from '@/types/action';
import { useActionsContext } from '@/contexts/ActionContext';

// Mock data for Actions
const MOCK_ACTIONS = [
  { id: '1', method: 'GET', path: '/users', tag: 'Users', description: 'Получить список всех пользователей системы.' },
  { id: '2', method: 'POST', path: '/users', tag: 'Users', description: 'Создать нового пользователя.' },
  { id: '3', method: 'GET', path: '/orders', tag: 'Orders', description: 'Запросить историю последних заказов.' },
  { id: '4', method: 'POST', path: '/emails/send', tag: 'Communication', description: 'Отправить email уведомление клиенту.' },
  { id: '5', method: 'PATCH', path: '/inventory/{id}', tag: 'Warehouse', description: 'Обновить остатки товара на складе.' },
  { id: '6', method: 'GET', path: '/analytics/daily', tag: 'Analytics', description: 'Получить отчет по продажам за сегодня.' },
  { id: '7', method: 'DELETE', path: '/users/{id}', tag: 'Users', description: 'Удалить пользователя навсегда.' },
];

const Actions: React.FC = () => {
  const {
    actions,
    searchTerm,
    setSearchTerm,
    filteredActions,
    addActions
  } = useActionsContext();
  
  const [isImportModalOpen, setIsImportModalOpen] = useState(false);
  const [isResultsModalOpen, setIsResultsModalOpen] = useState(false);
  const [importResults, setImportResults] = useState<{ success_actions: Action[], failed_actions: any[] } | null>(null);

  const getMethodColor = (method: string) => {
    switch (method) {
      case 'GET': return 'bg-green-500/10 text-green-500 border-green-500/20';
      case 'POST': return 'bg-blue-500/10 text-blue-500 border-blue-500/20';
      case 'PATCH': return 'bg-yellow-500/10 text-yellow-500 border-yellow-500/20';
      case 'DELETE': return 'bg-red-500/10 text-red-500 border-red-500/20';
      default: return 'bg-gray-500/10 text-gray-500 border-gray-500/20';
    }
  };

  return (
    <div className="flex h-full flex-col px-6 py-8">
      {/* Header Section */}
      <div className="flex flex-col md:flex-row md:items-center justify-between mb-8 gap-4">
        <div>
          <h1 className="text-2xl font-semibold text-foreground">Actions Library</h1>
          <p className="text-muted-foreground mt-1 text-sm">
            Библиотека технических эндпоинтов, импортированных из ваших OpenAPI спецификаций.
          </p>
        </div>
        <div className="flex items-center gap-3">
          <Button
            className="gap-2 bg-primary text-primary-foreground hover:bg-primary/90"
            onClick={() => setIsImportModalOpen(true)}
          >
            <FileJson className="h-4 w-4" />
            Import Swagger
          </Button>
        </div>
      </div>

      {/* Filters Section */}
      <div className="bg-card border border-border rounded-xl p-4 mb-6">
        <div className="relative max-w-md">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
          <Input
            placeholder="Поиск по пути или тегу..."
            className="pl-10 bg-background border-border"
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
          />
        </div>
      </div>

      {/* Table Section */}
      <div className="flex-1 rounded-xl bg-card border border-border shadow-sm overflow-hidden flex flex-col">
        <div className="overflow-auto flex-1">
          <Table>
            <TableHeader className="bg-muted/50 sticky top-0 z-10">
              <TableRow className="hover:bg-transparent border-none">
                <TableHead className="w-[100px] text-foreground">Method</TableHead>
                <TableHead className="text-foreground">Endpoint Path</TableHead>
                <TableHead className="text-foreground">Category</TableHead>
                <TableHead className="hidden md:table-cell text-foreground">Description</TableHead>
                <TableHead className="w-[50px]"></TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {filteredActions.length > 0 ? (
                filteredActions.map((action) => (
                  <TableRow key={action.id} className="group border-border">
                    <TableCell>
                      <Badge variant="outline" className={`${getMethodColor(action.method)} font-bold`}>
                        {action.method}
                      </Badge>
                    </TableCell>
                    <TableCell className="font-mono text-sm text-foreground">
                      {action.path}
                    </TableCell>
                    <TableCell>
                      <span className="text-xs px-2 py-1 rounded-full bg-secondary text-secondary-foreground border border-border">
                        {action.tags?.[0] || action.source_filename?.split('.')[0] || 'General'}
                      </span>
                    </TableCell>
                    <TableCell className="hidden md:table-cell text-muted-foreground text-sm max-w-xs truncate">
                      {action.summary || action.description}
                    </TableCell>
                    <TableCell>
                      <DropdownMenu>
                        <DropdownMenuTrigger asChild>
                          <Button variant="ghost" size="sm" className="h-8 w-8 p-0 opacity-0 group-hover:opacity-100 transition-opacity">
                            <MoreHorizontal className="h-4 w-4" />
                          </Button>
                        </DropdownMenuTrigger>
                        <DropdownMenuContent align="end" className="w-40 bg-card border-border">
                          <DropdownMenuItem className="gap-2 cursor-pointer focus:bg-accent">
                            <ChevronRight className="h-4 w-4" /> View Specs
                          </DropdownMenuItem>
                          <DropdownMenuItem className="gap-2 cursor-pointer focus:bg-accent">
                            <ExternalLink className="h-4 w-4" /> Test API
                          </DropdownMenuItem>
                          <DropdownMenuItem className="gap-2 cursor-pointer text-red-500 focus:bg-red-500/10 focus:text-red-500">
                            <Trash2 className="h-4 w-4" /> Delete
                          </DropdownMenuItem>
                        </DropdownMenuContent>
                      </DropdownMenu>
                    </TableCell>
                  </TableRow>
                ))
              ) : (
                <TableRow className="hover:bg-transparent">
                  <TableCell colSpan={5} className="h-64 text-center">
                    <div className="flex flex-col items-center justify-center py-12">
                      <div className="bg-muted/50 p-4 rounded-full mb-4">
                        <FileJson className="h-10 w-10 text-muted-foreground" />
                      </div>
                      <h3 className="text-lg font-medium text-foreground mb-1">Методы еще не загружены</h3>
                      <p className="text-sm text-muted-foreground mb-6 max-w-xs mx-auto">
                        Импортируйте вашу OpenAPI спецификацию, чтобы начать использовать API действия в ваших пайплайнах.
                      </p>
                      <Button
                        onClick={() => setIsImportModalOpen(true)}
                        className="gap-2"
                      >
                        <FileJson className="h-4 w-4" />
                        Import Swagger
                      </Button>
                    </div>
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </div>

        {/* Pagination Placeholder */}
        <div className="p-4 border-t border-border bg-muted/30 flex items-center justify-between text-xs text-muted-foreground">
          <span>Показано {filteredActions.length} из {actions.length} действий</span>
          <div className="flex gap-2">
            <Button variant="outline" size="sm" className="h-7 text-xs border-border" disabled>Back</Button>
            <Button variant="outline" size="sm" className="h-7 text-xs border-border" disabled>Next</Button>
          </div>
        </div>
      </div>
      <SwaggerImportModal
        isOpen={isImportModalOpen}
        onClose={() => setIsImportModalOpen(false)}
        onImport={(data) => {
          if (data && (data.success_actions || data.actions)) {
            const successList = data.success_actions || data.actions || [];
            const failedList = data.failed_actions || [];
            
            // Update main table with successful actions
            addActions(successList);
            
            // Prepare and open results modal
            setImportResults({
              success_actions: successList,
              failed_actions: failedList
            });
            setIsResultsModalOpen(true);
          }
        }}
      />
      <ImportResultsModal
        isOpen={isResultsModalOpen}
        onClose={() => setIsResultsModalOpen(false)}
        results={importResults}
      />
    </div>
  );
};

export default Actions;
