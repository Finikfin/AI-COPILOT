import React, { useState } from 'react';
import {
  Plus,
  Search,
  FileJson,
  MoreHorizontal,
  Trash2,
  ExternalLink,
  ChevronRight,
  FolderIcon
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
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

  const groupedActions = React.useMemo(() => {
    return filteredActions.reduce((acc, action) => {
      const filename = action.source_filename || (action.tags && action.tags[0]) || 'General Library';
      if (!acc[filename]) acc[filename] = [];
      acc[filename].push(action);
      return acc;
    }, {} as Record<string, Action[]>);
  }, [filteredActions]);
  
  const [isImportModalOpen, setIsImportModalOpen] = useState(false);
  const [isResultsModalOpen, setIsResultsModalOpen] = useState(false);
  const [importResults, setImportResults] = useState<{ succeeded_actions: Action[], failed_actions: any[] } | null>(null);

  const getMethodColor = (method: string) => {
    switch (method) {
      case 'GET': return 'bg-green-500/10 text-green-500 border-green-500/20';
      case 'POST': return 'bg-blue-500/10 text-blue-500 border-blue-500/20';
      case 'PUT': return 'bg-cyan-500/10 text-cyan-500 border-cyan-500/20';
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

      {/* Grouped Table Sections */}
      <div className="flex-1 rounded-xl bg-card border border-border shadow-sm overflow-hidden flex flex-col">
        <div className="overflow-auto flex-1 p-2">
          {Object.keys(groupedActions).length > 0 ? (
            <Accordion type="multiple" defaultValue={Object.keys(groupedActions)} className="space-y-4">
              {Object.entries(groupedActions).map(([filename, groupActions]) => (
                <AccordionItem 
                  key={filename} 
                  value={filename}
                  className="border border-border rounded-xl overflow-hidden bg-background/50"
                >
                  <AccordionTrigger className="px-4 py-3 hover:no-underline hover:bg-muted/50 transition-colors group">
                    <div className="flex items-center gap-3">
                      <div className="p-2 rounded-lg bg-primary/10 text-primary group-hover:scale-110 transition-transform">
                        <FileJson className="h-4 w-4" />
                      </div>
                      <div className="flex flex-col items-start gap-0.5">
                        <span className="text-sm font-semibold text-foreground">{filename}</span>
                        <span className="text-[10px] text-muted-foreground uppercase tracking-wider font-medium">
                          {groupActions.length} эндпоинтов
                        </span>
                      </div>
                    </div>
                  </AccordionTrigger>
                  <AccordionContent className="p-0 border-t border-border">
                    <Table>
                      <TableHeader className="bg-muted/30">
                        <TableRow className="hover:bg-transparent border-none">
                          <TableHead className="w-[100px] text-foreground h-10 py-0">Method</TableHead>
                          <TableHead className="text-foreground h-10 py-0">Endpoint Path</TableHead>
                          <TableHead className="hidden md:table-cell text-foreground h-10 py-0">Description</TableHead>
                          <TableHead className="w-[50px] h-10 py-0"></TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {groupActions.map((action) => (
                          <TableRow key={action.id} className="group border-border/50">
                            <TableCell className="py-2">
                              <Badge variant="outline" className={`${getMethodColor(action.method)} font-bold text-[10px] px-1.5 py-0`}>
                                {action.method}
                              </Badge>
                            </TableCell>
                            <TableCell className="font-mono text-[13px] text-foreground py-2">
                              {action.path}
                            </TableCell>
                            <TableCell className="hidden md:table-cell text-muted-foreground text-[13px] max-w-xs truncate py-2">
                              {action.summary || action.description}
                            </TableCell>
                            <TableCell className="py-2">
                              <DropdownMenu>
                                <DropdownMenuTrigger asChild>
                                  <Button variant="ghost" size="sm" className="h-8 w-8 p-0 opacity-0 group-hover:opacity-100 transition-opacity">
                                    <MoreHorizontal className="h-4 w-4" />
                                  </Button>
                                </DropdownMenuTrigger>
                                <DropdownMenuContent align="end" className="w-40 bg-card border-border">
                                  <DropdownMenuItem className="gap-2 cursor-pointer focus:bg-accent text-xs">
                                    <ChevronRight className="h-3 w-3" /> View Specs
                                  </DropdownMenuItem>
                                  <DropdownMenuItem className="gap-2 cursor-pointer focus:bg-accent text-xs">
                                    <ExternalLink className="h-3 w-3" /> Test API
                                  </DropdownMenuItem>
                                  <DropdownMenuItem className="gap-2 cursor-pointer text-red-500 focus:bg-red-500/10 focus:text-red-500 text-xs">
                                    <Trash2 className="h-3 w-3" /> Delete
                                  </DropdownMenuItem>
                                </DropdownMenuContent>
                              </DropdownMenu>
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </AccordionContent>
                </AccordionItem>
              ))}
            </Accordion>
          ) : (
            <div className="flex flex-col items-center justify-center py-20 bg-background/50 m-2 rounded-xl border border-dashed border-border">
              <div className="bg-muted/50 p-4 rounded-full mb-4">
                <FileJson className="h-10 w-10 text-muted-foreground" />
              </div>
              <h3 className="text-lg font-medium text-foreground mb-1">Методы еще не загружены</h3>
              <p className="text-sm text-muted-foreground mb-6 max-w-xs mx-auto text-center">
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
          )}
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
          if (data && (data.succeeded_actions || data.actions)) {
            const successList = data.succeeded_actions || data.actions || [];
            const failedList = data.failed_actions || [];
            
            // Update main table with successful actions
            addActions(successList);
            
            // Prepare and open results modal
            setImportResults({
              succeeded_actions: successList,
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
