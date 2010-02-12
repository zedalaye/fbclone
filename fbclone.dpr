program fbclone;

{$APPTYPE CONSOLE}
{$R *.res}

uses
  Windows,
  SysUtils,
  Classes,
  Generics.Collections,
  UIB,
  UIBLib,
  UIBase,
  UIBMetadata,
  UIBConst,
  CxGetOpts;

type
  ConsoleString = type AnsiString(850);
  TScriptResult = (srNothingDone, srEchec, srSucces, srAnnule);

  TDatabase = record
    ConnectionString: string;
    Username: string;
    Password: string;
  end;

  TPerformanceCounter = record
  type
    TDuration = record
      value: Extended;
      class operator Implicit(E: Extended): TDuration; {$IFDEF RELEASE}inline;{$ENDIF}
      class operator Add(V1, V2: TDuration): TDuration; {$IFDEF RELEASE}inline;{$ENDIF}
      class operator Add(V1: TDuration; V2: Extended): TDuration; {$IFDEF RELEASE}inline;{$ENDIF}
      function ToString: String;
    end;
  private
    counting: Boolean;
    frequency: Int64;
    start_time: Int64;
  public
    duration: TDuration;
    last_duration: TDuration;
    count: Cardinal;
    last_count: Cardinal;
    procedure Start; {$IFDEF RELEASE}inline;{$ENDIF}
    function Stop: Extended; {$IFDEF RELEASE}inline;{$ENDIF}
    procedure Reset;
  end;

  TPerformanceStats = record
    transaction_setup: TPerformanceCounter;
    transaction_commit: TPerformanceCounter;
    prepare: TPerformanceCounter;
    drop: TPerformanceCounter;
    reading: TPerformanceCounter;
    reading_prepare: TPerformanceCounter;
    reading_commit: TPerformanceCounter;
    writing: TPerformanceCounter;
    blobs: TPerformanceCounter;
    arrays: TPerformanceCounter;
    function LastToString: String;
    function GlobalToString: String;
    procedure Reset;
  end;

  TOverallPerformanceStats = record
    duration: TPerformanceCounter;
    source_connection: TPerformanceCounter;
    source_disconnection: TPerformanceCounter;
    target_connection: TPerformanceCounter;
    target_disconnection: TPerformanceCounter;
    target_drop: TPerformanceCounter;
    target_creation: TPerformanceCounter;
    source_metadata: TPerformanceCounter;
    data_pump: TPerformanceCounter;
    meta_roles: TPerformanceCounter;
    meta_functions: TPerformanceCounter;
    meta_domains: TPerformanceCounter;
    meta_generators: TPerformanceCounter;
    meta_exceptions: TPerformanceCounter;
    meta_procedures: TPerformanceCounter;
    meta_tables: TPerformanceCounter;
    meta_views: TPerformanceCounter;
    meta_unique: TPerformanceCounter;
    meta_primary: TPerformanceCounter;
    meta_foreign: TPerformanceCounter;
    meta_indices: TPerformanceCounter;
    meta_triggers: TPerformanceCounter;
    meta_checks: TPerformanceCounter;
    meta_grants: TPerformanceCounter;
  end;

{ TPerformanceStats }

procedure TPerformanceStats.Reset;
begin
  transaction_setup.Reset;
  transaction_commit.Reset;
  prepare.Reset;
  drop.Reset;
  reading.Reset;
  reading_prepare.Reset;
  reading_commit.Reset;
  writing.Reset;
  blobs.Reset;
  arrays.Reset;
end;

function TPerformanceStats.LastToString: String;
begin
  Result :=
    Format(
      '  Transaction Setup    [%9d] %s' + #13#10 +
      '  Transaction Commit   [%9d] %s' + #13#10 +
      '  Statement Prepare    [%9d] %s' + #13#10 +
      '  Statement Drop       [%9d] %s' + #13#10 +
      '  Source Prepare       [%9d] %s' + #13#10 +
      '  Source Data Read     [%9d] %s' + #13#10 +
      '  Source Data Commit   [%9d] %s' + #13#10 +
      '  Target Data Write    [%9d] %s' + #13#10 +
      '  Target Blob Create   [%9d] %s' + #13#10 +
      '  Target Arrays Create [%9d] %s',
      [

        transaction_setup.last_count, transaction_setup.last_duration.ToString,
        transaction_commit.last_count, transaction_commit.last_duration.ToString,
        prepare.last_count, prepare.last_duration.ToString,
        drop.last_count, drop.last_duration.ToString,
        reading_prepare.last_count, reading_prepare.last_duration.ToString,
        reading.last_count, reading.last_duration.ToString,
        reading_commit.last_count, reading_commit.last_duration.ToString,
        writing.last_count, writing.last_duration.ToString,
        blobs.last_count, blobs.last_duration.ToString,
        arrays.last_count, arrays.last_duration.ToString
      ]);
end;

function TPerformanceStats.GlobalToString: String;
begin
  Result :=
    Format(
      '  Transaction Setup    [%9d] %s' + #13#10 +
      '  Transaction Commit   [%9d] %s' + #13#10 +
      '  Statement Prepare    [%9d] %s' + #13#10 +
      '  Statement Drop       [%9d] %s' + #13#10 +
      '  Source Prepare       [%9d] %s' + #13#10 +
      '  Source Data Read     [%9d] %s' + #13#10 +
      '  Source Data Commit   [%9d] %s' + #13#10 +
      '  Target Data Write    [%9d] %s' + #13#10 +
      '  Target Blob Create   [%9d] %s' + #13#10 +
      '  Target Arrays Create [%9d] %s',
      [
        transaction_setup.count, transaction_setup.duration.ToString,
        transaction_commit.count, transaction_commit.duration.ToString,
        prepare.count, prepare.duration.ToString,
        drop.count, drop.duration.ToString,
        reading_prepare.count, reading_prepare.duration.ToString,
        reading.count, reading.duration.ToString,
        reading_commit.count, reading_commit.duration.ToString,
        writing.count, writing.duration.ToString,
        blobs.count, blobs.duration.ToString,
        arrays.count, arrays.duration.ToString
      ]);
end;

{ TPerformanceCounter }

procedure TPerformanceCounter.Start;
begin
  Assert(not counting, 'Cannot Start : Already Counting !');
  counting := true;
  Inc(count);
  Inc(last_count);
  frequency := 0;
  start_time := 0;
  QueryPerformanceFrequency(frequency);
  QueryPerformanceCounter(start_time);
end;

function TPerformanceCounter.Stop: Extended;
var
  end_time: Int64;
  d: Extended;
begin
  Assert(counting, 'Cannot Stop : Not Counting !');
  QueryPerformanceCounter(end_time);
  d := (end_time - start_time) / frequency;
  last_duration := last_duration + d;
  duration := duration + d;
  Result := d;
  counting := false;
end;

procedure TPerformanceCounter.Reset;
begin
  Assert(not counting, 'Cannot Reset : Already Counting !');
  last_duration.value := 0;
  last_count := 0;
end;

{ TPerformanceCounter.TDuration }

class operator TPerformanceCounter.TDuration.Add(V1, V2: TDuration): TDuration;
begin
  Result.value := V1.value + V2.value;
end;

class operator TPerformanceCounter.TDuration.Add(V1: TDuration;
  V2: Extended): TDuration;
begin
  Result.value := V1.value + V2;
end;

class operator TPerformanceCounter.TDuration.Implicit(E: Extended): TDuration;
begin
  Result.value := E;
end;

function TPerformanceCounter.TDuration.ToString: String;
var
  h,m,s: Integer;
  ms: Extended;
  mss: string;
begin
  s := Trunc(value);
  m := s div 60;
  s := s - (m * 60);
  h := m div 60;
  m := m - (h * 60);
  ms := Frac(value);

  mss := Format('%.4f', [ms]);
  while mss[1] <> DecimalSeparator do
    Delete(mss,1,1);

  if (h > 0) or (m > 0) or (s > 0) then
    Result := Format('%.2d:%.2d:%.2d%s h', [h, m, s, mss])
  else if ms > 0.00001 then
    Result := Format('%13.4f ms', [ms * 1000])
  else
    Result := Format('%13d ms', [0]);
end;

procedure TraceAction(const Action: ConsoleString; Resultat: TScriptResult);
begin
  if Resultat = srEchec then
    WriteLn(ErrOutput, Action)
  else
    WriteLn(Action);
end;

procedure Trace(const Action: String = '');
begin
  TraceAction(ConsoleString(Action), srSucces);
end;

procedure TraceError(const Error: String = '');
begin
  TraceAction(ConsoleString(Error), srEchec);
end;

function Clone(const Source, Target: TDatabase; PageSize: Integer; const DataCharset, MetaCharset: string; Verbose: Boolean; PumpOnly: Boolean; FailSafe: Boolean; CommitInterval: Integer; const DumpFile: string): Boolean;
var
  SrcDatabase, DstDatabase: TUIBDatabase;
  SrcTransaction, DstTransaction: TUIBTransaction;
  SrcQuery: TUIBQuery;
  FErrorsCount: Integer;
  SQLDump: TStringStream;
  Perfs: TOverallPerformanceStats;

  procedure AddLog(const What: String = ''; Resultat: TScriptResult = srSucces); overload;
  begin
    if Verbose then
      TraceAction(ConsoleString(What), Resultat);
  end;

  procedure AddLog(const FmtStr: String; const Args: array of const; Resultat: TScriptResult = srSucces); overload;
  begin
    AddLog(Format(FmtStr, Args), Resultat);
  end;

  procedure PumpData(dbhandle: IscDbHandle; mdb: TMetaDataBase;
    charset: TCharacterSet; failsafe: Boolean; commit_interval: Integer; sorttables: Boolean);
  var
    T,F,c,l: Integer;
    done: Integer;
    sql: string;
    trhandle: IscTrHandle;
    sthandle: IscStmtHandle;
    blhandle: IscBlobHandle;
    Table: TMetaTable;
    S: TPerformanceStats;
    LastDisplay: Cardinal;
    first_blob: Integer;

    function TablesCount: Integer;
    begin
      if sorttables then
        Result := mdb.SortedTablesCount
      else
        Result := mdb.TablesCount;
    end;

    function Tables(const Index: Integer): TMetaTable;
    begin
      if sorttables then
        Result := mdb.SortedTables[Index]
      else
        Result := mdb.Tables[Index];
    end;

    procedure LogError(E: EUIBError; Q: TUIBQuery);
    var
      c: Integer;
    begin
      AddLog('--- failed ---', srEchec);
      AddLog('ErrorCode = %d' + #13#10 + 'SQLCode = %d', [E.ErrorCode, E.SQLCode], srEchec);
      AddLog(e.Message, srEchec);
      AddLog('--- source fields values ---', srEchec);
      for c := 0 to Q.Fields.FieldCount - 1 do
        case Q.Fields.FieldType[c] of
          uftBlob, uftBlobId:
            AddLog('%s = [BLOB]', [Q.Fields.AliasName[c]], srEchec);
          uftArray:
            AddLog('%s = [ARRAY]', [Q.Fields.AliasName[c]], srEchec)
          else
            AddLog('%s = %s', [Q.Fields.AliasName[c], Q.Fields.AsString[c]], srEchec);
        end;
      AddLog('--- rolling back record and continue ---', srEchec);
    end;

    procedure CheckDstTransaction;
    begin
      if not DstTransaction.InTransaction then
      begin
        S.transaction_setup.Start;
          DstTransaction.StartTransaction;
          trhandle := DstTransaction.TrHandle;
        S.transaction_setup.Stop;
      end;
    end;

  begin
    for T := 0 to TablesCount - 1 do
    try
      S.Reset;

      Table := Tables(T);
      AddLog('  Table %s', [Table.Name]);

      first_blob := 0;
      sql := 'select ';
      c := 0;
      for F := 0 to Table.FieldsCount - 1 do
        if Table.Fields[F].ComputedSource = '' then
        begin
          if Table.Fields[F].FieldType in [uftBlob, uftBlobId] then
            first_blob := F;

          if (c = 0) then
            sql := sql + Table.Fields[F].Name
          else
            sql := sql + ', ' + Table.Fields[F].Name;
          inc(c);
        end;
      sql := sql + ' from ' + Table.Name;

      if PumpOnly and (Table.PrimaryCount > 0) then
      begin
        c := 0;
        for F := 0 to Table.Primary[0].FieldsCount - 1 do
        begin
          if (c = 0) then
            sql := sql + ' order by '
          else
            sql := sql + ', ';
          sql := sql + Table.Primary[0].Fields[F].Name;
          Inc(c);
        end;
      end;

      done := 0;
      LastDisplay := 0;

      S.reading_prepare.Start;
        SrcQuery.SQL.Text := sql;
        SrcQuery.Prepare(true);
      S.reading_prepare.Stop;

      S.reading.Start;
        SrcQuery.Open;
      S.reading.Stop;

      if not (SrcQuery.Eof) then
      begin
        sql := format('INSERT INTO %s (%s', [Table.Name, Table.MetaQuote(SrcQuery.Fields.SqlName[0])]);
        for F := 1 to SrcQuery.Fields.FieldCount - 1 do
          sql := sql + ', ' + Table.MetaQuote(SrcQuery.Fields.SqlName[F]);
        sql := sql + ') VALUES (?';
        for F := 1 to SrcQuery.Fields.FieldCount - 1 do
          sql := sql + ',?';
        sql := sql + ');';

        with DstDatabase.Lib do
        begin
          CheckDstTransaction;

          S.prepare.Start;
            sthandle := nil;
            DSQLAllocateStatement(dbhandle, sthandle);
            DSQLPrepare(dbhandle, trhandle, sthandle,  MBUEncode(sql, CharacterSetCP[charset]), 3, nil);
          S.prepare.Stop;

          while not SrcQuery.Eof do
          begin
            // recreate blobs
            if first_blob > 0 then
              for F := first_blob to SrcQuery.Fields.FieldCount - 1 do
                case SrcQuery.Fields.FieldType[F] of
                  uftBlob, uftBlobId:
                    begin
                      if (not SrcQuery.Fields.IsNull[F]) then
                      begin
                        CheckDstTransaction;
                        S.blobs.Start;
                          blhandle := nil;
                          TSQLDA(SrcQuery.Fields).AsQuad[F] := BlobCreate(dbhandle, trhandle, blhandle);
                          BlobWriteSegment(blhandle, SrcQuery.Fields.BlobData[F].Size, SrcQuery.Fields.BlobData[F].Buffer);
                          BlobClose(blhandle);
                        S.blobs.Stop;
                      end;
                    end
                end;

            // recreate array
            for F := 0 to SrcQuery.Fields.ArrayCount - 1 do
              if (not SrcQuery.Fields.IsNull[SrcQuery.Fields.ArrayInfos[F].index]) then
              begin
                CheckDstTransaction;
                S.arrays.Start;
                  l := SrcQuery.Fields.ArrayInfos[F].index;
                  TSQLDA(SrcQuery.Fields).AsQuad[l] := QuadNull;
                  TSQLDA(SrcQuery.Fields).IsNull[l] := false;
                  ArrayPutSlice(dbhandle, trhandle, PGDSQuad(SrcQuery.Fields.Data.sqlvar[l].SqlData)^, SrcQuery.Fields.ArrayInfos[F].info, SrcQuery.Fields.ArrayData[l], SrcQuery.Fields.ArrayInfos[F].size);
                S.arrays.Stop;
              end;

            try
              CheckDstTransaction;
              S.writing.Start;
                DSQLExecute(trhandle, sthandle, 3, SrcQuery.Fields);
              S.writing.Stop;

              Inc(done);

              if failsafe or (done mod commit_interval = 0) then
              begin
                S.transaction_commit.Start;
                  DstTransaction.Commit;
                S.transaction_commit.Stop;
              end;

              if Verbose and (GetTickCount - LastDisplay > 100) then
              begin
                Write('    ', done, ' records' + #13);
                LastDisplay := GetTickCount;
              end;
            except
              on E: EUIBError do
              begin
                LogError(E, SrcQuery);
                S.transaction_commit.Start;
                  DstTransaction.Commit;
                S.transaction_commit.Stop;
                Inc(FErrorsCount);
              end;
            end;

            S.reading.Start;
              SrcQuery.Next;
            S.reading.Stop;
          end;

          if DstTransaction.InTransaction then
          begin
            try
              S.transaction_commit.Start;
                DstTransaction.Commit;
              S.transaction_commit.Stop;
            except
              on E: EUIBError do
              begin
                LogError(E, SrcQuery);
                Inc(FErrorsCount);
              end;
            end;
          end;

          S.drop.Start;
            DSQLFreeStatement(sthandle, DSQL_drop);
          S.drop.Stop;
        end;
      end;

      S.reading_commit.Start;
        SrcQuery.Close(etmCommit);
      S.reading_commit.Stop;

      AddLog('    %d records', [done]);
      AddLog(#13#10 +
             'Stats' + #13#10 +
             S.LastToString + #13#10);
    except
      on E: Exception do
      begin
        AddLog('--- failed ---', srEchec);
        AddLog(e.Message, srEchec);
        AddLog('--------------', srEchec);
        Inc(FErrorsCount);
        Continue;
      end;
    end;

    AddLog(#13#10 + 'Global Stats');
    AddLog(S.GlobalToString);
  end;

var
  dbhandle: IscDbHandle;
  metadb: TMetaDataBase;
  data_charset, meta_charset: TCharacterSet;
  i, j: integer;

  procedure ExecuteImmediate(const SQL: string);
  begin
    try
      DstTransaction.ExecuteImmediate(sql);
      DstTransaction.Commit;
    except
      on e: Exception do
      begin
        AddLog('--- failed ---', srEchec);
        AddLog(sql, srEchec);
        AddLog('---  exception  ---', srEchec);
        AddLog(e.Message, srEchec);
        AddLog('--------------', srEchec);
        inc(FErrorsCount);
      end;
    end;
  end;

begin
  Perfs.duration.Start;

  Result := true;

  FErrorsCount := 0;

  if (MetaCharset <> '') then
    meta_charset := StrToCharacterSet(RawByteString(MetaCharset))
  else
    meta_charset := csNONE;

  SrcDatabase := TUIBDataBase.Create(nil);
  SrcDatabase.DatabaseName := Source.ConnectionString;
  SrcDatabase.UserName := Source.Username;
  SrcDatabase.PassWord := Source.Password;
  SrcDatabase.CharacterSet := meta_charset;
  SrcDatabase.Params.Add('no_garbage_collect');

  try
    Perfs.source_connection.Start;
      SrcDatabase.Connected := true;
    Perfs.source_connection.Stop;

    AddLog('Source connection' + #13#10 +
           '  Database %s'  + #13#10 +
           '  Username %s'  + #13#10 +
           '  Page Size %d' + #13#10 +
           '  Client %s'    + #13#10 +
           '  Server %s',
      [
        Source.ConnectionString,
        Source.Username,
        SrcDatabase.InfoPageSize,
        SrcDatabase.InfoVersion,
        SrcDatabase.InfoFirebirdVersion
      ]
    );
  except
    on E: EUIBError do
    begin
      AddLog('Cannot connect source database %s' + #13#10 +
             '%s', [Source.ConnectionString, E.Message], srEchec);
      Exit;
    end;
  end;

  DstDatabase := TUIBDataBase.Create(nil);
  DstDatabase.DatabaseName := Target.ConnectionString;
  DstDatabase.UserName := Target.Username;
  DstDatabase.PassWord := Target.Password;
  DstDatabase.CharacterSet := meta_charset;
  DstDatabase.Params.Add('set_page_buffers=50');

  try
    Perfs.target_connection.Start;
      DstDatabase.Connected := true;
    Perfs.target_connection.Stop;

    if PumpOnly then
    begin
      AddLog(#13#10 +
             'Target connection' + #13#10 +
             '  Database %s'  + #13#10 +
             '  Username %s'  + #13#10 +
             '  Page Size %d' + #13#10 +
             '  Client %s'    + #13#10 +
             '  Server %s',
        [
          Target.ConnectionString,
          Target.Username,
          DstDatabase.InfoPageSize,
          DstDatabase.InfoVersion,
          DstDatabase.InfoFirebirdVersion
        ]
      );
    end
    else
    begin
      try
        Perfs.target_drop.Start;
          DstDatabase.DropDatabase;
        Perfs.target_drop.Stop;
      except
        on E: EUIBError do
        begin
          WriteLn('Impossible de supprimer la base de données cible ' + Target.ConnectionString + #13#10 +
                  E.Message);
          Exit;
        end;
      end;
    end;
  except
    { si on ne peut pas se connecter c'est que la base n'existe pas ! tout baigne }
    Perfs.target_connection.Stop;
  end;

  metadb := TMetaDataBase.Create(nil,-1);

  SrcTransaction := TUIBTransaction.Create(nil);
  SrcQuery := TUIBQuery.Create(nil);
  SrcQuery.CachedFetch := false;

  DstTransaction := TUIBTransaction.Create(nil);
  try
    SrcTransaction.Options := [tpRead, tpConcurrency];
    SrcTransaction.DataBase := SrcDatabase;
    SrcQuery.Transaction := SrcTransaction;
    SrcQuery.FetchBlobs := true;

    DstTransaction.Options := [tpWrite, tpReadCommitted, tpNoAutoUndo];
    DstTransaction.DataBase := DstDatabase;

    if meta_charset = csNONE then
    begin
      { En NONE on ne se permet de récupérer QUE le charset par défaut de la
        base de données, ensuite on se reconnecte avec le charset par défaut }
      metadb.OIDDatabases := [OIDDBCharset];

      Perfs.source_metadata.Start;
        metadb.LoadFromDatabase(SrcTransaction);
      Perfs.source_metadata.Stop;

      meta_charset := metadb.DefaultCharset;
      metadb.Free;
      metadb := TMetaDatabase.Create(nil, -1);

      Perfs.source_disconnection.Start;
        SrcDatabase.Connected := false;
      Perfs.source_disconnection.Stop;

      SrcDatabase.CharacterSet := meta_charset;

      Perfs.source_connection.Start;
        SrcDatabase.Connected := true;
      Perfs.source_connection.Stop;
    end;

    { Lit l'intégralité des métadonnées }
    metadb.OIDDatabases := ALLObjects;

    Perfs.source_metadata.Start;
      metadb.LoadFromDatabase(SrcTransaction);
    Perfs.source_metadata.Stop;

    { Ecrit le script SQL de création de la base de données dans le fichier }
    if DumpFile <> '' then
    begin
      SQLDump := TStringStream.Create;
      try
        metadb.SaveToDDL(SQLDump, [ddlFull]);
        SQLDump.SaveToFile(DumpFile);
      finally
        SQLDump.Free;
      end;
    end;

    if DataCharset = '' then
      data_charset := metadb.DefaultCharset
    else
      data_charset := StrToCharacterSet(RawByteString(DataCharset));

    if data_charset <> meta_charset then
    begin
      Perfs.source_disconnection.Start;
        SrcDatabase.Connected := false;
      Perfs.source_disconnection.Stop;

      SrcDatabase.CharacterSet := data_charset;

      Perfs.source_connection.Start;
        SrcDatabase.Connected := true;
      Perfs.source_connection.Stop;
    end;

    if not PumpOnly then
    begin
      if PageSize = 0 then
        PageSize := SrcDatabase.InfoPageSize;

      DstDatabase.CharacterSet := data_charset;

      Perfs.target_creation.Start;
        DstDatabase.CreateDatabase(data_charset, PageSize);
      Perfs.target_creation.Stop;

      AddLog(#13#10 +
             'Create Target'  + #13#10 +
             '  Database %s'  + #13#10 +
             '  Username %s'  + #13#10 +
             '  Page Size %d' + #13#10 +
             '  Client %s'    + #13#10 +
             '  Server %s',
        [
          Target.ConnectionString,
          Target.Username,
          DstDatabase.InfoPageSize,
          DstDatabase.InfoVersion,
          DstDatabase.InfoFirebirdVersion
        ]
      );

      // ROLES
      AddLog(#13#10 + 'Create Roles');
      for i := 0 to metadb.RolesCount - 1 do
      begin
        AddLog('  %s', [metadb.Roles[i].Name]);
        Perfs.meta_roles.Start;
          ExecuteImmediate(metadb.Roles[i].AsFullDDL);
        Perfs.meta_roles.Stop;
      end;

      // UDF
      AddLog(#13#10 + 'Create UDF');
      for i := 0 to metadb.UDFSCount - 1 do
      begin
        AddLog('  %s', [metadb.UDFS[i].Name]);
        Perfs.meta_functions.Start;
          ExecuteImmediate(metadb.UDFS[i].AsFullDDL);
        Perfs.meta_functions.Stop;
      end;

      // DOMAINS
      AddLog(#13#10 + 'Create Domains');
      for i := 0 to metadb.DomainsCount - 1 do
      begin
        AddLog('  %s', [metadb.Domains[i].Name]);
        Perfs.meta_domains.Start;
          ExecuteImmediate(metadb.Domains[i].AsFullDDL);
        Perfs.meta_domains.Stop;
      end;

      // GENERATORS
      AddLog(#13#10 + 'Create Generators');
      for i := 0 to metadb.GeneratorsCount - 1 do
      begin
        AddLog('  %s', [metadb.Generators[i].Name]);
        Perfs.meta_generators.Start;
          ExecuteImmediate(metadb.Generators[i].AsCreateDLL);
        Perfs.meta_generators.Stop;
      end;

      // EXEPTIONS
      AddLog(#13#10 + 'Create Exceptions');
      for i := 0 to metadb.ExceptionsCount - 1 do
      begin
        AddLog('  %s', [metadb.Exceptions[i].Name]);
        Perfs.meta_exceptions.Start;
          ExecuteImmediate(metadb.Exceptions[i].AsFullDDL);
        Perfs.meta_exceptions.Stop;
      end;

      // EMPTY PROCEDURES
      AddLog(#13#10 + 'Create Empty Procedures');
      for i := 0 to metadb.ProceduresCount - 1 do
      begin
        AddLog('  %s', [metadb.Procedures[i].Name]);
        Perfs.meta_procedures.Start;
          ExecuteImmediate(metadb.Procedures[i].AsCreateEmptyDDL);
        Perfs.meta_procedures.Stop;
      end;

      // TABLES
      AddLog(#13#10 + 'Create Tables');
      for i := 0 to metadb.TablesCount - 1 do
      begin
        AddLog('  %s', [metadb.Tables[i].Name]);
        Perfs.meta_tables.Start;
          ExecuteImmediate(metadb.Tables[i].AsFullDDLNode);
        Perfs.meta_tables.Stop;
      end;

      // VIEWS
      AddLog(#13#10 + 'Create Views');
      for i := 0 to metadb.ViewsCount - 1 do
      begin
        AddLog('  %s', [metadb.Views[i].Name]);
        Perfs.meta_views.Start;
          ExecuteImmediate(metadb.Views[i].AsFullDDLNode);
        Perfs.meta_views.Stop;
      end;
    end;

    // TABLES DATA
    AddLog(#13#10 + 'Pump Data');
    dbhandle := DstDatabase.DbHandle;
    DstTransaction.Commit;

    Perfs.data_pump.Start;
      PumpData(dbhandle, metadb, data_charset, failsafe, CommitInterval, PumpOnly); // Sort Tables by Foreign Keys in case of Data Pump (constraints already exists)
    Perfs.data_pump.Stop;

    // GENERATORS VALUES
    AddLog(#13#10 + 'Sync Generators');
    for i := 0 to metadb.GeneratorsCount - 1 do
    begin
      AddLog('  %s', [metadb.Generators[i].Name]);
      Perfs.meta_generators.Start;
        ExecuteImmediate(metadb.Generators[i].AsAlterDDL);
      Perfs.meta_generators.Stop;
    end;

    if not PumpOnly then
    begin
      // UNIQUE
      AddLog(#13#10 + 'Create Unique Constraints');
      for i := 0 to metadb.TablesCount - 1 do
      for j := 0 to metadb.Tables[i].UniquesCount - 1 do
      begin
        AddLog('  %s', [metadb.Tables[i].Uniques[j].Name]);
        Perfs.meta_unique.Start;
          ExecuteImmediate(metadb.Tables[i].Uniques[j].AsFullDDL);
        Perfs.meta_unique.Stop;
      end;

      // PRIMARY
      AddLog(#13#10 + 'Create Primary Keys Constraints');
      for i := 0 to metadb.TablesCount - 1 do
      for j := 0 to metadb.Tables[i].PrimaryCount - 1 do
      begin
        AddLog('  %s', [metadb.Tables[i].Primary[j].Name]);
        Perfs.meta_primary.Start;
          ExecuteImmediate(metadb.Tables[i].Primary[j].AsFullDDL);
        Perfs.meta_primary.Stop;
      end;

      // FOREIGN
      AddLog(#13#10 + 'Create Foreign Keys Constraints');
      for i := 0 to metadb.TablesCount - 1 do
      for j := 0 to metadb.Tables[i].ForeignCount - 1 do
      begin
        AddLog('  %s', [metadb.Tables[i].Foreign[j].Name]);
        Perfs.meta_foreign.Start;
          ExecuteImmediate(metadb.Tables[i].Foreign[j].AsFullDDL);
        Perfs.meta_foreign.Stop;
      end;

      // INDICES
      AddLog(#13#10 + 'Create Indices');
      for i := 0 to metadb.TablesCount - 1 do
      for j := 0 to metadb.Tables[i].IndicesCount - 1 do
      begin
        AddLog('  %s', [metadb.Tables[i].Indices[j].Name]);
        Perfs.meta_indices.Start;
          ExecuteImmediate(metadb.Tables[i].Indices[j].AsDDL);
          if not metadb.Tables[i].Indices[j].Active then
            ExecuteImmediate(metadb.Tables[i].Indices[j].AsAlterToInactiveDDL);
        Perfs.meta_indices.Stop;
      end;

      // CHECKS
      AddLog(#13#10 + 'Create Check Constraints');
      for i := 0 to metadb.TablesCount - 1 do
      for j := 0 to metadb.Tables[i].ChecksCount - 1 do
      begin
        AddLog('  %s', [metadb.Tables[i].Checks[j].Name]);
        Perfs.meta_checks.Start;
          ExecuteImmediate(metadb.Tables[i].Checks[j].AsFullDDL);
        Perfs.meta_checks.Stop;
      end;

      // TABLE TRIGGERS
      AddLog(#13#10 + 'Create Triggers');
      for i := 0 to metadb.TablesCount - 1 do
      for j := 0 to metadb.Tables[i].TriggersCount - 1 do
      begin
        AddLog('  %s', [metadb.Tables[i].Triggers[j].Name]);
        Perfs.meta_triggers.Start;
          ExecuteImmediate(metadb.Tables[i].Triggers[j].AsFullDDL);
        Perfs.meta_triggers.Stop;
      end;

      // VIEW TRIGGERS
      AddLog(#13#10 + 'Create Views');
      for i := 0 to metadb.ViewsCount - 1 do
      for j := 0 to metadb.Views[i].TriggersCount - 1 do
      begin
        AddLog('  %s', [metadb.Views[i].Triggers[j].Name]);
        Perfs.meta_triggers.Start;
          ExecuteImmediate(metadb.Views[i].Triggers[j].AsFullDDL);
        Perfs.meta_triggers.Stop;
      end;

      // ALTER PROCEDURES
      AddLog(#13#10 + 'Create Procedures Code');
      for i := 0 to metadb.ProceduresCount - 1 do
      begin
        AddLog('  %s', [metadb.Procedures[i].Name]);
        Perfs.meta_procedures.Start;
          ExecuteImmediate(metadb.Procedures[i].AsAlterDDL);
        Perfs.meta_procedures.Stop;
      end;

      // GRANTS               AddLog(#13#10 + 'Grant Roles');
      for i := 0 to metadb.RolesCount - 1 do
      begin
        for j := 0 to metadb.Roles[i].GrantsCount - 1 do
        begin
           AddLog('  %s', [metadb.Roles[i].Grants[j].Name]);
           Perfs.meta_grants.Start;
             ExecuteImmediate(metadb.Roles[i].Grants[j].AsFullDDL);
           Perfs.meta_grants.Stop;
        end;
      end;

      AddLog(#13#10 + 'Grant Tables and Fields');
      for i := 0 to metadb.TablesCount - 1 do
      begin
        for j := 0 to metadb.Tables[i].GrantsCount - 1 do
        begin
          AddLog('  Table %s', [metadb.Tables[i].Grants[j].Name]);
          Perfs.meta_grants.Start;
            ExecuteImmediate(metadb.Tables[i].Grants[j].AsFullDDL);
          Perfs.meta_grants.Stop;
        end;
        for j := 0 to metadb.Tables[i].FieldsGrantsCount - 1 do
        begin
          AddLog('  Field: %s', [metadb.Tables[i].FieldsGrants[j].Name]);
          Perfs.meta_grants.Start;
            ExecuteImmediate(metadb.Tables[i].FieldsGrants[j].AsFullDDL);
          Perfs.meta_grants.Stop;
        end;
      end;

      AddLog(#13#10 + 'Grant Views and Fields');
      for i := 0 to metadb.ViewsCount - 1 do
      begin
        for j := 0 to metadb.Views[i].GrantsCount - 1 do
        begin
          AddLog('  View %s', [metadb.Views[i].Grants[j].Name]);
          Perfs.meta_grants.Start;
            ExecuteImmediate(metadb.Views[i].Grants[j].AsFullDDL);
          Perfs.meta_grants.Stop;
        end;
        for j := 0 to metadb.Views[i].FieldsGrantsCount - 1 do
        begin
          AddLog('  Field %s', [metadb.Views[i].FieldsGrants[j].Name]);
          Perfs.meta_grants.Start;
            ExecuteImmediate(metadb.Tables[i].FieldsGrants[j].AsFullDDL);
          Perfs.meta_grants.Stop;
        end;
      end;

      AddLog(#13#10 + 'Grant Procedures');
      for i := 0 to metadb.ProceduresCount - 1 do
      begin
        for j := 0 to metadb.Procedures[i].GrantsCount - 1 do
        begin
          AddLog('  %s', [metadb.Procedures[i].Grants[j].Name]);
          Perfs.meta_grants.Start;
            ExecuteImmediate(metadb.Procedures[i].Grants[j].AsFullDDL);
          Perfs.meta_grants.Stop;
        end;
      end;
    end;
  finally
    metadb.Free;
    SrcQuery.Free;
    SrcTransaction.Free;
    SrcDatabase.Free;
    DstTransaction.Free;
    DstDatabase.Free;
    if FErrorsCount > 0 then
    begin
      AddLog('--- %d error(s) ! ---', [FErrorsCount], srEchec);
      Result := False;
    end;

    Perfs.duration.Stop;

    AddLog('Overall Counters');
    AddLog(' Duration           %s', [Perfs.duration.duration.ToString]);
    AddLog(' Data Pump          %s', [Perfs.data_pump.duration.ToString]);
    AddLog(' Source Database');
    AddLog('  Connection        %s', [Perfs.source_connection.duration.ToString]);
    AddLog('  Disconnection     %s', [Perfs.source_disconnection.duration.ToString]);
    AddLog('  Metadata Loading  %s', [Perfs.source_metadata.duration.ToString]);
    AddLog(' Target Database');
    AddLog('  Connection        %s', [Perfs.target_connection.duration.ToString]);
    AddLog('  Disconnection     %s', [Perfs.target_disconnection.duration.ToString]);
    AddLog('  Drop              %s', [Perfs.target_drop.duration.ToString]);
    AddLog('  Creation          %s', [Perfs.target_creation.duration.ToString]);
    if not PumpOnly then
    begin
      AddLog(' Metadata Cloning');
      AddLog('  Roles             %s', [Perfs.meta_roles.duration.ToString]);
      AddLog('  User Functions    %s', [Perfs.meta_functions.duration.ToString]);
      AddLog('  Domains           %s', [Perfs.meta_domains.duration.ToString]);
      AddLog('  Generators        %s', [Perfs.meta_generators.duration.ToString]);
      AddLog('  Exceptions        %s', [Perfs.meta_exceptions.duration.ToString]);
      AddLog('  Procedures        %s', [Perfs.meta_procedures.duration.ToString]);
      AddLog('  Tables            %s', [Perfs.meta_tables.duration.ToString]);
      AddLog('  Views             %s', [Perfs.meta_views.duration.ToString]);
      AddLog('  Unique Index      %s', [Perfs.meta_unique.duration.ToString]);
      AddLog('  Primary Keys      %s', [Perfs.meta_primary.duration.ToString]);
      AddLog('  Foreign Keys      %s', [Perfs.meta_foreign.duration.ToString]);
      AddLog('  Indices           %s', [Perfs.meta_indices.duration.ToString]);
      AddLog('  Triggers          %s', [Perfs.meta_triggers.duration.ToString]);
      AddLog('  Check Constraints %s', [Perfs.meta_checks.duration.ToString]);
      AddLog('  Grants            %s', [Perfs.meta_grants.duration.ToString]);
    end;
  end;
end;

procedure MapEnvironment(var D: TDatabase);

  function ReadEnvironment(const Name: string): string;
  var
    BufSize: Integer;  // buffer size required for value
  begin
    // Get required buffer size (inc. terminal #0)
    BufSize := GetEnvironmentVariable(PChar(Name), nil, 0);
    if BufSize > 0 then
    begin
      // Read env var value into result string
      SetLength(Result, BufSize - 1);
      GetEnvironmentVariable(PChar(Name), PChar(Result), BufSize);
    end
    else
      // No such environment variable
      Result := '';
  end;

begin
  if D.Username = '' then
    D.Username := ReadEnvironment('ISC_USER');
  if D.Password = '' then
    D.Password := ReadEnvironment('ISC_PASSWORD');
end;

var
  GO: TGetOpt;
  O: POption;
  P: TPair<POption, string>;
  src, tgt: TDatabase;
  data_charset, meta_charset: string;
  verbose: Boolean;
  dump_file: string;
  pump_only: Boolean;
  failsafe: Boolean;
  commit_interval: integer;
  page_size: Integer;
begin
  verbose := false;
  pump_only := false;
  failsafe := False;
  commit_interval := 10000;
  meta_charset := '';
  dump_file := '';
  page_size := 0;

  GO := TGetOpt.Create;
  try
    try
      GO.RegisterFlag('h',    'help',            '', 'Show this help message', false);
      GO.RegisterFlag('v',    'verbose',         '', 'Show details', false);

      GO.RegisterFlag('po',   'pump-only',       '', 'Pump data only from source database into target database (source database and target database must chare the same metadata structure)', false);

      GO.RegisterSwitch('d',  'dump',            'file',      'Dump SQL script into file', false);

      GO.RegisterSwitch('ps', 'page-size',       'page size', 'Overrides target database page size', false);

      GO.RegisterSwitch('s',  'source',          'database', 'Source database connection string', true);
      GO.RegisterSwitch('su', 'source-user',     'user',     'Source database user name', false);
      GO.RegisterSwitch('sp', 'source-password', 'password', 'Source database password', false);

      GO.RegisterSwitch('t',  'target',          'database', 'Target database connection string', true);
      GO.RegisterSwitch('tu', 'target-user',     'user',     'Target database user name', false);
      GO.RegisterSwitch('tp', 'target-password', 'password', 'Target database password', false);

      GO.RegisterSwitch('c',  'charset',          'charset', 'Character set for data transfer', false);
      GO.RegisterSwitch('mc', 'metadata-charset', 'charset', 'Character set used to access source database metadata', false);

      GO.RegisterSwitch('u',  'user',     'user',     'Source and target databases user name', false);
      GO.RegisterSwitch('p',  'password', 'password', 'Source and target databases password', false);

      GO.RegisterFlag('f',    'failsafe',        '',          'Commit transaction every record (same as using -ci 1)', false);
      GO.RegisterSwitch('ci', 'commit-interval', 'number',    'Commit transaction every <number> record', false);

      GO.Parse;

      if GO.Flag['h'] then
      begin
        Trace(GO.PrintSyntax);
        Trace;
        Trace(GO.PrintHelp);
        Exit;
      end;

      if not GO.Validate then
      begin
        TraceError('Missing parameters on command line:');
        for O in GO.Missing do
          TraceError(' ' + O.ToShortSyntax);
        TraceError;
        TraceError(GO.PrintSyntax);
        Halt(1);
      end;

      if GO.Flag['u'] and (GO.Flag['su'] or GO.Flag['tu']) then
      begin
        TraceError('Conflict between parameters on command line:');
        TraceError(' Flags -tu and -su cannot be used with -u');
        TraceError;
        TraceError(GO.PrintSyntax);
        Halt(1);
      end;

      if GO.Flag['p'] and (GO.Flag['sp'] or GO.Flag['tp']) then
      begin
        TraceError('Conflict between parameters on command line:');
        TraceError(' Flags -tp and -sp cannot be used with -p');
        TraceError;
        TraceError(GO.PrintSyntax);
        Halt(1);
      end;

      if GO.Flag['po'] and GO.Flag['ps'] then
      begin
        TraceError('Useless flag on command line:');
        TraceError(' The flag -ps (Page Size) will be ignored if -po (Pump Only Mode) is specified');
        TraceError;
      end;

      if GO.Flag['f'] and GO.Flag['ci'] then
      begin
        TraceError('Useless flag on command line:');
        TraceError(' The flag -ci (Commit Interval) will be ignored if -f (Failsafe Mode) is specified');
        TraceError;
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
        else if P.Key^.Short = 'su' then
          src.Username := P.Value
        else if P.Key^.Short = 'sp' then
          src.Password := P.Value
        else if P.Key^.Short = 't' then
          tgt.ConnectionString := P.Value
        else if P.Key^.Short = 'tu' then
          tgt.Username := P.Value
        else if P.Key^.Short = 'tp' then
          tgt.Password := P.Value
        else if P.Key^.Short = 'c' then
          data_charset := P.Value
        else if P.Key^.Short = 'mc' then
          meta_charset := P.Value
        else if P.Key^.Short = 'v' then
          verbose := true
        else if P.Key^.Short = 'd' then
          dump_file := P.Value
        else if P.Key^.Short = 'po' then
          pump_only := true
        else if P.Key^.Short = 'f' then
          failsafe := true
        else if P.Key^.Short = 'ci' then
          commit_interval := StrToInt(P.Value)
        else if P.Key^.Short = 'ps' then
          page_size := StrToInt(P.Value);
      end;

      MapEnvironment(src);
      MapEnvironment(tgt);

    except
      on E: Exception do
      begin
        TraceError('Error on command line ' + E.Message);
        TraceError(GO.PrintSyntax);
        Halt(1);
      end;
    end;
  finally
    GO.Free;
  end;

  try
    if not Clone(src, tgt, page_size,
             data_charset, meta_charset,
             verbose,
             pump_only,
             failsafe, commit_interval,
             dump_file)
    then
      Halt(1);
  except
    on E: Exception do
    begin
      TraceError('General exception ' + E.Message);
      Halt(1);
    end;
  end;
end.

