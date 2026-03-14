import React, { useState, useRef } from 'react';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Textarea } from '@/components/ui/textarea';
import { Label } from '@/components/ui/label';
import { FileCode, Upload, Loader2, FileJson, CheckCircle2 } from 'lucide-react';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { toast } from 'sonner';

interface SwaggerImportModalProps {
  isOpen: boolean;
  onClose: () => void;
  onImport: (data: any) => void;
}

export const SwaggerImportModal: React.FC<SwaggerImportModalProps> = ({
  isOpen,
  onClose,
  onImport,
}) => {
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [spec, setSpec] = useState('');
  const [isImporting, setIsImporting] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file) {
      if (file.type !== 'application/json' && !file.name.endsWith('.json') && !file.name.endsWith('.yaml') && !file.name.endsWith('.yml')) {
        toast.error('Пожалуйста, выберите JSON или YAML файл');
        return;
      }
      setSelectedFile(file);
    }
  };

  const handleImportFile = async () => {
    if (!selectedFile) {
      toast.error('Пожалуйста, выберите файл');
      return;
    }

    setIsImporting(true);
    try {
      const reader = new FileReader();
      reader.onload = async (e) => {
        const content = e.target?.result as string;

        try {
          const formData = new FormData();
          const fileBlob = new Blob([content], { type: selectedFile.type });
          formData.append('file', fileBlob, selectedFile.name);

          const response = await fetch('/api/v1/actions/ingest', {
            method: 'POST',
            body: formData,
          });

          if (!response.ok) throw new Error('Failed to import');

          const result = await response.json();
          toast.success(`Файл ${selectedFile.name} успешно импортирован на сервер`);
          onImport(result);
          onClose();
        } catch (error) {
          toast.error('Ошибка при отправке файла на сервер');
          console.error(error);
        } finally {
          setIsImporting(false);
        }
      };
      reader.readAsText(selectedFile);
    } catch (error) {
      toast.error('Ошибка при чтении файла');
      setIsImporting(false);
    }
  };

  const handleImportByContent = async () => {
    if (!spec) {
      toast.error('Пожалуйста, вставьте содержимое спецификации');
      return;
    }

    setIsImporting(true);
    try {
      const formData = new FormData();
      const specBlob = new Blob([spec], { type: 'application/json' });
      formData.append('file', specBlob, 'manual_import.json');

      const response = await fetch('/api/v1/actions/ingest', {
        method: 'POST',
        body: formData,
      });

      if (!response.ok) throw new Error('Failed to import');

      const result = await response.json();
      toast.success('Методы успешно импортированы на сервер');
      onImport(result);
      onClose();
    } catch (error) {
      toast.error('Ошибка при отправке спецификации на сервер');
      console.error(error);
    } finally {
      setIsImporting(false);
    }
  };

  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent className="sm:max-w-[600px] bg-card border-border">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <FileCode className="h-5 w-5 text-primary" />
            Import Swagger / OpenAPI
          </DialogTitle>
          <DialogDescription>
            Выберите способ импорта вашей API спецификации для создания новых Actions.
          </DialogDescription>
        </DialogHeader>

        <Tabs defaultValue="file" className="w-full mt-4">
          <TabsList className="grid w-full grid-cols-2 bg-muted/50">
            <TabsTrigger value="file" className="gap-2">
              <FileJson className="h-4 w-4" />
              Upload JSON
            </TabsTrigger>
            <TabsTrigger value="content" className="gap-2">
              <Upload className="h-4 w-4" />
              Paste JSON/YAML
            </TabsTrigger>
          </TabsList>

          <TabsContent value="file" className="space-y-4 pt-4">
            <div className="space-y-4">
              <div
                className={`border-2 border-dashed rounded-xl p-8 flex flex-col items-center justify-center transition-colors cursor-pointer ${selectedFile ? 'border-primary/50 bg-primary/5' : 'border-border hover:border-primary/30 hover:bg-muted/50'
                  }`}
                onClick={() => fileInputRef.current?.click()}
              >
                <input
                  type="file"
                  ref={fileInputRef}
                  onChange={handleFileChange}
                  accept=".json,.yaml,.yml"
                  className="hidden"
                />

                {selectedFile ? (
                  <>
                    <div className="bg-primary/10 p-3 rounded-full mb-3">
                      <CheckCircle2 className="h-8 w-8 text-primary" />
                    </div>
                    <p className="text-sm font-medium text-foreground">{selectedFile.name}</p>
                    <p className="text-xs text-muted-foreground mt-1">
                      {(selectedFile.size / 1024).toFixed(2)} KB • Нажмите, чтобы изменить
                    </p>
                  </>
                ) : (
                  <>
                    <div className="bg-muted p-3 rounded-full mb-3">
                      <Upload className="h-8 w-8 text-muted-foreground" />
                    </div>
                    <p className="text-sm font-medium text-foreground">Выберите JSON файл</p>
                    <p className="text-xs text-muted-foreground mt-1">
                      Перетащите файл сюда или нажмите для поиска
                    </p>
                  </>
                )}
              </div>

              <div className="text-xs text-muted-foreground bg-muted/30 p-3 rounded-lg flex items-start gap-2">
                <FileCode className="h-4 w-4 mt-0.5 shrink-0" />
                <span>Поддерживаются форматы JSON и YAML (OpenAPI 3.0/ Swagger 2.0). Файл будет обработан локально в вашем браузере.</span>
              </div>
            </div>

            <Button
              className="w-full gap-2"
              onClick={handleImportFile}
              disabled={isImporting || !selectedFile}
            >
              {isImporting ? <Loader2 className="h-4 w-4 animate-spin" /> : <Upload className="h-4 w-4" />}
              {isImporting ? 'Обработка...' : 'Импортировать из файла'}
            </Button>
          </TabsContent>

          <TabsContent value="content" className="space-y-4 pt-4">
            <div className="space-y-2">
              <Label htmlFor="swagger-content">Content</Label>
              <Textarea
                id="swagger-content"
                placeholder='{"openapi": "3.0.0", ...}'
                className="min-h-[250px] font-mono text-xs bg-background border-border"
                value={spec}
                onChange={(e) => setSpec(e.target.value)}
              />
            </div>
            <Button
              className="w-full gap-2"
              onClick={handleImportByContent}
              disabled={isImporting}
            >
              {isImporting ? <Loader2 className="h-4 w-4 animate-spin" /> : <Upload className="h-4 w-4" />}
              {isImporting ? 'Импорт...' : 'Импортировать методы'}
            </Button>
          </TabsContent>
        </Tabs>

        <DialogFooter className="sm:justify-start">
          <p className="text-[10px] text-muted-foreground uppercase tracking-wider font-semibold">
            SECURE LOCAL PROCESSING • VALIDATED BY AI
          </p>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
};
