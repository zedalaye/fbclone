(*
 * The contents of this file are subject to the
 * Initial Developer's Public License Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License from the Firebird Project website,
 * at http://www.firebirdsql.org/index.php?op=doc&id=idpl.
 *
 * Software distributed under the License is distributed on an "AS IS" basis,
 * WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
 * for the specific language governing rights and limitations under the License.
 *
 * The Original Code was created by Pierre Yager and Henri Gourvest.
 *)

program fbclone;

{$APPTYPE CONSOLE}
{$R *.res}

uses
  Windows,
  SysUtils,
  Classes,
  Generics.Collections,
  uib,
  uiblib,
  uibase,
  uibmetadata,
  uibconst,
  console.getopts in 'console.getopts.pas',
  fbclone.cloner in 'fbclone.cloner.pas',
{$IFDEF ENABLE_BENCHMARK}
  fbclone.benchmark in 'fbclone.benchmark.pas',
{$ENDIF}
  fbclone.database in 'fbclone.database.pas',
  fbclone.logger in 'fbclone.logger.pas';

procedure BackupRepairFile(const repair_file_name: string);
var
  repair_file_ext: string;
  backup_file_name: string;
begin
  if FileExists(repair_file_name) then
  begin
    repair_file_ext := ExtractFileExt(repair_file_name);
    if repair_file_ext <> '' then
      Insert('~', repair_file_ext, 2);
    backup_file_name := ChangeFileExt(repair_file_name, repair_file_ext);
    if FileExists(backup_file_name) then
      DeleteFile(backup_file_name);
    RenameFile(repair_file_name, backup_file_name);
  end;
end;

procedure RegisterExcludedTables(Cloner: TCloner; const TablesList: string);
var
  L: TStringList;
  S: String;
begin
  L := TStringList.Create;
  try
    L.Delimiter := ',';
    L.DelimitedText := TablesList;
    for S in L do
      Cloner.AddExcludedTable(S);
  finally
    L.Free;
  end;
end;

var
  GO: TGetOpt;
  O: POption;
  P: TPair<POption, string>;
  src, tgt: TDatabase;
  target_charset, read_charset, write_charset: string;
  opts: TClonerOptions;
  dump_file, repair_file: string;
  commit_interval: integer;
  page_size: Integer;
  excluded_tables: string;
  c: TCloner;
  l: ILogger;
begin
  opts := [];
  commit_interval := 10000;
  target_charset := '';
  read_charset := '';
  write_charset := '';
  dump_file := '';
  page_size := 0;

  l := TConsoleLogger.Create;

  GO := TGetOpt.Create;
  try
    try
      GO.RegisterFlag('h',    'help',            '', 'Show this help message', false);
      GO.RegisterFlag('v',    'verbose',         '', 'Show details', false);

      GO.RegisterFlag('po',   'pump-only',       '', 'Only pump data from source database into target database (source database and target database must share the same metadata structure)', false);
      GO.RegisterFlag('e',    'empty-tables',    '', 'Empty tables before data pump', false);

      GO.RegisterSwitch('d',  'dump',            'file', 'Dump SQL script into file', false);
      GO.RegisterSwitch('rd', 'repair-dump',     'file', 'Trace errors and SQL into a repair.sql file', false);

      GO.RegisterSwitch('ps', 'page-size',       'page size', 'Overrides target database page size', false);

      GO.RegisterSwitch('s',  'source',          'database', 'Source database connection string', true);
      GO.RegisterSwitch('su', 'source-user',     'user',     'User name used to connect source database', false);
      GO.RegisterSwitch('sp', 'source-password', 'password', 'Password used to connect source database', false);
      GO.RegisterSwitch('sl', 'source-library',  'library',  'Client Library used to connect source database', false);

      GO.RegisterSwitch('t',  'target',          'database', 'Target database connection string', true);
      GO.RegisterSwitch('tu', 'target-user',     'user',     'User name used to connect target database', false);
      GO.RegisterSwitch('tp', 'target-password', 'password', 'Password used to connect target dat base', false);
      GO.RegisterSwitch('tl', 'target-library',  'library',  'Client Library used to connect target database', false);
      GO.RegisterSwitch('tc', 'target-charset',  'charset',  'Target database default character set (default: source charset)', false);

      GO.RegisterSwitch('rc', 'read-charset',    'charset', 'Character set to read from source database (default: source charset)', false);
      GO.RegisterSwitch('wc', 'write-charset',   'charset', 'Character set to write into target database (default: source charset)', false);

      GO.RegisterSwitch('xt', 'exclude-table',   'list', 'Comma separated list of tables to exclude from data pump', false);

      GO.RegisterSwitch('u',  'user',     'user',     'User name used to connect both source and target databases', false);
      GO.RegisterSwitch('p',  'password', 'password', 'Password used to connect both source and target databases', false);
      GO.RegisterSwitch('l',  'library',  'library',  'Client Library used to connect both source and target databases', false);

      GO.RegisterFlag('f',    'failsafe',        '',          'Commit transaction every record (same as using -ci 1)', false);
      GO.RegisterSwitch('ci', 'commit-interval', 'number',    'Commit transaction every <number> record', false);

      GO.Parse;

      if GO.Flag['h'] then
      begin
        l.Info(GO.PrintSyntax);
        l.Info;
        l.Info(GO.PrintHelp);
        Exit;
      end;

      if not GO.Validate then
      begin
        l.Error('Missing parameters on command line:');
        for O in GO.Missing do
          l.Error(' ' + O.ToShortSyntax);
        l.Error;
        l.Error(GO.PrintSyntax);
        Halt(1);
      end;

      if GO.Flag['u'] and (GO.Flag['su'] or GO.Flag['tu']) then
      begin
        l.Error('Conflict between parameters on command line:');
        l.Error(' Flags -tu and -su cannot be used with -u');
        l.Error;
        l.Error(GO.PrintSyntax);
        Halt(1);
      end;

      if GO.Flag['p'] and (GO.Flag['sp'] or GO.Flag['tp']) then
      begin
        l.Error('Conflict between parameters on command line:');
        l.Error(' Flags -tp and -sp cannot be used with -p');
        l.Error;
        l.Error(GO.PrintSyntax);
        Halt(1);
      end;

      if GO.Flag['l'] and (GO.Flag['sl'] or GO.Flag['tl']) then
      begin
        l.Error('Conflict between parameters on command line:');
        l.Error(' Flags -tl and -sl cannot be used with -l');
        l.Error;
        l.Error(GO.PrintSyntax);
        Halt(1);
      end;

      if GO.Flag['po'] and GO.Flag['ps'] then
      begin
        l.Error('Useless flag on command line:');
        l.Error(' The flag -ps (Page Size) will be ignored if -po (Pump Only Mode) is specified');
        l.Error;
      end;

      if GO.Flag['po'] and GO.Flag['tc'] then
      begin
        l.Error('Useless flag on command line:');
        l.Error(' The flag -tc (Target Character Set) will be ignored if -po (Pump Only Mode) is specified');
        l.Error;
      end;

      if GO.Flag['e'] and (not GO.Flag['po']) then
      begin
        l.Error('Useless flag on command line:');
        l.Error(' The flag -e (Empty Tables) will be ignored if -po (Pump Only Mode) is not specified');
        l.Error;
      end;

      if GO.Flag['f'] and GO.Flag['ci'] then
      begin
        l.Error('Useless flag on command line:');
        l.Error(' The flag -ci (Commit Interval) will be ignored if -f (Failsafe Mode) is specified');
        l.Error;
      end;

      for P in GO.Parameters do
      begin
        if P.Key^.Short = 's' then
          src.ConnectionString := P.Value
        else if P.Key^.Short = 'u' then
        begin
          src.Username := P.Value;
          tgt.Username := P.Value;
        end
        else if P.Key^.Short = 'p' then
        begin
          src.Password := P.Value;
          tgt.Password := P.Value;
        end
        else if P.Key^.Short = 'l' then
        begin
          src.LibraryFileName := P.Value;
          tgt.LibraryFileName := P.Value;
        end
        else if P.Key^.Short = 'su' then
          src.Username := P.Value
        else if P.Key^.Short = 'sp' then
          src.Password := P.Value
        else if P.Key^.Short = 'sl' then
          src.LibraryFileName := P.Value
        else if P.Key^.Short = 't' then
          tgt.ConnectionString := P.Value
        else if P.Key^.Short = 'tu' then
          tgt.Username := P.Value
        else if P.Key^.Short = 'tp' then
          tgt.Password := P.Value
        else if P.Key^.Short = 'tl' then
          tgt.LibraryFileName := P.Value
        else if P.Key^.Short = 'tc' then
          target_charset := P.Value
        else if P.Key^.Short = 'rc' then
          read_charset := P.Value
        else if P.Key^.Short = 'wc' then
          write_charset := P.Value
        else if P.Key^.Short = 'v' then
          Include(opts, coVerbose)
        else if P.Key^.Short = 'd' then
          dump_file := P.Value
        else if P.Key^.Short = 'rd' then
          repair_file := P.Value
        else if P.Key^.Short = 'po' then
          Include(opts, coPumpOnly)
        else if P.Key^.Short = 'e' then
          Include(opts, coEmptyTables)
        else if P.Key^.Short = 'f' then
          Include(opts, coFailSafe)
        else if P.Key^.Short = 'ci' then
          commit_interval := StrToInt(P.Value)
        else if P.Key^.Short = 'ps' then
          page_size := StrToInt(P.Value)
        else if P.Key^.Short = 'xt' then
          excluded_tables := P.Value
      end;

      MapEnvironment(src);
      MapEnvironment(tgt);

      if repair_file <> '' then
        BackupRepairFile(repair_file);
    except
      on E: Exception do
      begin
        l.Error('Error on command line ' + E.Message);
        l.Error(GO.PrintSyntax);
        Halt(1);
      end;
    end;
  finally
    GO.Free;
  end;

  try
    c := TCloner.Create;
    try
      c.Logger := l;
      c.PageSize := page_size;
      c.TargetCharset := target_charset;
      c.ReadCharset := read_charset;
      c.WriteCharset := write_charset;
      c.Options := opts;
      c.CommitInterval := commit_interval;
      c.DumpFile := dump_file;
      c.RepairFile := repair_file;

      RegisterExcludedTables(c, excluded_tables);

      if not c.Clone(src, tgt) then
        Halt(1);
    finally
      c.Free;
    end;

  {$IFDEF DEBUG}
    WriteLn;
    WriteLn('[DEBUG] Press a key to terminate');
    ReadLn;
  {$ENDIF}
  except
    on E: Exception do
    begin
      l.Error('General exception ' + E.Message);
      Halt(1);
    end;
  end;
end.

