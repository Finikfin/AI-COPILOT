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
    success_actions: any[];
    failed_actions: any[];
  } | null;
}

export const ImportResultsModal: React.FC<ImportResultsModalProps> = ({
  isOpen,
  onClose,
  results,
}) => {
  if (!results) return null;

  const totalSuccess = results.success_actions.length;
  const totalFailed = results.failed_actions.length;

  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent className="sm:max-w-[800px] max-h-[80vh] flex flex-col bg-card border-border">
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
          <div className="bg-green-500/10 border border-green-500/20 rounded-lg p-3 flex items-center gap-3">
            <CheckCircle2 className="h-5 w-5 text-green-500" />
            <div>
              <p className="text-sm font-medium text-green-500">Успешно</p>
              <p className="text-2xl font-bold text-foreground">{totalSuccess}</p>
            </div>
          </div>
          <div className="bg-red-500/10 border border-red-500/20 rounded-lg p-3 flex items-center gap-3">
            <XCircle className="h-5 w-5 text-red-500" />
            <div>
              <p className="text-sm font-medium text-red-500">Ошибки</p>
              <p className="text-2xl font-bold text-foreground">{totalFailed}</p>
            </div>
          </div>
        </div>

        <ScrollArea className="flex-1 pr-4">
          <div className="space-y-6">
            {/* Success Table */}
            {totalSuccess > 0 && (
              <div>
                <h3 className="text-sm font-semibold mb-3 flex items-center gap-2 text-green-500">
                  <CheckCircle2 className="h-4 w-4" />
                  Успешно загруженные методы
                </h3>
                <Table>
                  <TableHeader>
                    <TableRow className="hover:bg-transparent">
                      <TableHead className="w-[80px]">Метод</TableHead>
                      <TableHead>Путь</TableHead>
                      <TableHead>Описание</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {results.success_actions.map((action, idx) => (
                      <TableRow key={idx} className="hover:bg-transparent border-border/50">
                        <TableCell>
                          <Badge variant="outline" className="font-bold border-green-500/30 text-green-500 bg-green-500/5">
                            {action.method}
                          </Badge>
                        </TableCell>
                        <TableCell className="font-mono text-xs">{action.path}</TableCell>
                        <TableCell className="text-xs text-muted-foreground truncate max-w-[200px]">
                          {action.summary || action.operation_id}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>
            )}

            {/* Failed Table */}
            {totalFailed > 0 && (
              <div>
                <h3 className="text-sm font-semibold mb-3 flex items-center gap-2 text-red-500">
                  <XCircle className="h-4 w-4" />
                  Методы с ошибками
                </h3>
                <Table>
                  <TableHeader>
                    <TableRow className="hover:bg-transparent">
                      <TableHead className="w-[80px]">Метод</TableHead>
                      <TableHead>Путь</TableHead>
                      <TableHead>Причина / Ошибка</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {results.failed_actions.map((action, idx) => (
                      <TableRow key={idx} className="hover:bg-transparent border-border/50">
                        <TableCell>
                          <Badge variant="outline" className="font-bold border-red-500/30 text-red-500 bg-red-500/5">
                            {action.method || '???'}
                          </Badge>
                        </TableCell>
                        <TableCell className="font-mono text-xs">{action.path || 'N/A'}</TableCell>
                        <TableCell className="text-xs text-red-400 font-medium">
                          {action.error || 'Ошибка парсинга или валидации'}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
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
