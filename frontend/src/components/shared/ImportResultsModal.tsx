import React from 'react';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { Badge } from '@/components/ui/badge';
import { CheckCircle2, XCircle, AlertCircle } from 'lucide-react';
import { ScrollArea } from '@/components/ui/scroll-area';

interface ImportResultsModalProps {
  isOpen: boolean;
  onClose: () => void;
  results: {
    succeeded_actions: any[];
    failed_actions: any[];
  } | null;
}

export const ImportResultsModal: React.FC<ImportResultsModalProps> = ({
  isOpen,
  onClose,
  results,
}) => {
  if (!results) return null;

  const totalSuccess = results.succeeded_actions.length;
  const totalFailed = results.failed_actions.length;

  const getMethodStyle = (method: string) => {
    switch (method?.toUpperCase()) {
      case 'GET': return { color: 'hsl(142, 70%, 45%)', backgroundColor: 'hsl(142, 70%, 45% / 0.1)', borderColor: 'hsl(142, 70%, 45% / 0.2)' };
      case 'POST': return { color: 'hsl(217, 91%, 60%)', backgroundColor: 'hsl(217, 91%, 60% / 0.1)', borderColor: 'hsl(217, 91%, 60% / 0.2)' };
      case 'PUT': return { color: 'hsl(188, 86%, 45%)', backgroundColor: 'hsl(188, 86%, 45% / 0.1)', borderColor: 'hsl(188, 86%, 45% / 0.2)' };
      case 'PATCH': return { color: 'hsl(45, 93%, 47%)', backgroundColor: 'hsl(45, 93%, 47% / 0.1)', borderColor: 'hsl(45, 93%, 47% / 0.2)' };
      case 'DELETE': return { color: 'hsl(0, 84%, 60%)', backgroundColor: 'hsl(0, 84%, 60% / 0.1)', borderColor: 'hsl(0, 84%, 60% / 0.2)' };
      default: return { color: 'hsl(var(--muted-foreground))', backgroundColor: 'hsl(var(--muted))', borderColor: 'hsl(var(--border))' };
    }
  };

  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent className="sm:max-w-[800px] max-h-[90vh] flex flex-col bg-card border-border">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2 text-xl">
            <AlertCircle className="h-6 w-6 text-primary" />
            Результаты импорта
          </DialogTitle>
          <DialogDescription>
            Общий итог загрузки вашей спецификации.
          </DialogDescription>
        </DialogHeader>

        <div className="grid grid-cols-2 gap-4 mb-4">
          <div className="bg-green-500/5 border border-green-500/20 rounded-xl p-4 flex items-center gap-3">
            <div className="h-10 w-10 rounded-full bg-green-500/10 flex items-center justify-center">
              <CheckCircle2 className="h-6 w-6 text-green-500" />
            </div>
            <div>
              <p className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Успешно</p>
              <p className="text-2xl font-bold text-foreground">{totalSuccess}</p>
            </div>
          </div>
          <div className="bg-red-500/5 border border-red-500/20 rounded-xl p-4 flex items-center gap-3">
            <div className="h-10 w-10 rounded-full bg-red-500/10 flex items-center justify-center">
              <XCircle className="h-6 w-6 text-red-500" />
            </div>
            <div>
              <p className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Ошибки</p>
              <p className="text-2xl font-bold text-foreground">{totalFailed}</p>
            </div>
          </div>
        </div>

        <ScrollArea className="flex-1 pr-4">
          <div className="space-y-8">
            {/* Success Table */}
            {totalSuccess > 0 && (
              <div>
                <h3 className="text-sm font-semibold mb-4 px-1 flex items-center gap-2 text-green-500/80 uppercase tracking-tight">
                  <span className="w-1.5 h-1.5 rounded-full bg-green-500" />
                  Успешно загруженные методы
                </h3>
                <div className="rounded-md border border-border/40 overflow-hidden">
                  <Table>
                    <TableHeader className="bg-muted/30">
                      <TableRow className="hover:bg-transparent border-border/40">
                        <TableHead className="w-[100px] text-xs font-bold">Метод</TableHead>
                        <TableHead className="text-xs font-bold">Путь</TableHead>
                        <TableHead className="text-xs font-bold">Описание</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {results.succeeded_actions.map((action, idx) => (
                        <TableRow key={idx} className="hover:bg-muted/20 border-border/40 h-12">
                          <TableCell>
                            <Badge variant="outline" className="font-mono text-[10px]" style={getMethodStyle(action.method)}>
                              {action.method}
                            </Badge>
                          </TableCell>
                          <TableCell className="font-mono text-[11px] text-foreground/80">{action.path}</TableCell>
                          <TableCell className="text-[11px] text-muted-foreground italic truncate max-w-[250px]">
                            {action.summary || action.operation_id || 'Нет описания'}
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              </div>
            )}

            {/* Failed Table */}
            {totalFailed > 0 && (
              <div>
                <h3 className="text-sm font-semibold mb-4 px-1 flex items-center gap-2 text-red-500/80 uppercase tracking-tight">
                  <span className="w-1.5 h-1.5 rounded-full bg-red-500" />
                  Методы с ошибками
                </h3>
                <div className="rounded-md border border-border/40 overflow-hidden">
                  <Table>
                    <TableHeader className="bg-muted/30">
                      <TableRow className="hover:bg-transparent border-border/40">
                        <TableHead className="w-[100px] text-xs font-bold text-red-400">Метод</TableHead>
                        <TableHead className="text-xs font-bold">Путь</TableHead>
                        <TableHead className="text-xs font-bold">Причина / Ошибка</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {results.failed_actions.map((action, idx) => (
                        <TableRow key={idx} className="hover:bg-red-500/5 border-border/40 h-12">
                          <TableCell>
                            <Badge variant="outline" className="font-mono text-[10px] border-red-500/30 text-red-500 bg-red-500/5">
                              {action.method || '???'}
                            </Badge>
                          </TableCell>
                          <TableCell className="font-mono text-[11px] text-red-400/70">{action.path || 'N/A'}</TableCell>
                          <TableCell className="text-[11px] text-red-400/90 font-medium leading-tight">
                            {action.error || 'Ошибка парсинга или валидации'}
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              </div>
            )}
          </div>
        </ScrollArea>

        <DialogFooter className="mt-6">
          <Button onClick={onClose} className="w-full sm:w-auto">
            Ок
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
};
