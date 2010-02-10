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
  TScriptResult = (srNothingDone, srEchec, srSucces, srAnnule);

  TDatabase = record
    ConnectionString: string;
    Username: string;
    Password: string;
  end;

procedure TraceAction(const Action: String; Resultat: TScriptResult);
begin
  if Resultat = srEchec then
    WriteLn(ErrOutput, Action)
  else
    WriteLn(Action);
end;

function Clone(const Source, Target: TDatabase; PageSize: Integer; const DataCharset, MetaCharset: string; Verbose: Boolean; PumpOnly: Boolean; FailSafe: Boolean; CommitInterval: Integer; const DumpFile: string): Boolean;
var
  SrcDatabase, DstDatabase: TUIBDatabase;
  SrcTransaction, DstTransaction: TUIBTransaction;
  SrcQuery: TUIBQuery;
  FErrorsCount: Integer;
  SQLDump: TStringStream;

  procedure AddLog(const What: string; Resultat: TScriptResult = srSucces); overload;
  begin
    if Verbose then
      TraceAction(What, Resultat);
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
        DstTransaction.StartTransaction;
        trhandle := DstTransaction.TrHandle;
      end;
    end;

  begin
    for T := 0 to TablesCount - 1 do
    try
      Table := Tables(T);
      AddLog('  Table %s', [Table.Name]);
      sql := 'select ';
      c := 0;
      for F := 0 to Table.FieldsCount - 1 do
        if Table.Fields[F].ComputedSource = '' then
        begin
          if (c = 0) then
            sql := sql + Table.Fields[F].Name
          else
            sql := sql + ', ' + Table.Fields[F].Name;
          inc(c);
        end;
      sql := sql + ' from ' + Table.Name;
      if Table.PrimaryCount > 0 then
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
      SrcQuery.SQL.Text := sql;
      SrcQuery.Open;

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

          sthandle := nil;
          DSQLAllocateStatement(dbhandle, sthandle);
          DSQLPrepare(dbhandle, trhandle, sthandle,  MBUEncode(sql, CharacterSetCP[charset]), 3, nil);

          done := 0;
          while not SrcQuery.Eof do
          begin
            // recreate blobs
            for F := 0 to SrcQuery.Fields.FieldCount - 1 do
              case SrcQuery.Fields.FieldType[F] of
                uftBlob, uftBlobId:
                  begin
                    if (not SrcQuery.Fields.IsNull[F]) then
                    begin
                      CheckDstTransaction;
                      blhandle := nil;
                      TSQLDA(SrcQuery.Fields).AsQuad[F] := BlobCreate(dbhandle, trhandle, blhandle);
                      BlobWriteSegment(blhandle, SrcQuery.Fields.BlobData[F].Size, SrcQuery.Fields.BlobData[F].Buffer);
                      BlobClose(blhandle);
                    end;
                  end
              end;

            // recreate array
            for F := 0 to SrcQuery.Fields.ArrayCount - 1 do
              if (not SrcQuery.Fields.IsNull[SrcQuery.Fields.ArrayInfos[F].index]) then
              begin
                CheckDstTransaction;
                l := SrcQuery.Fields.ArrayInfos[F].index;
                TSQLDA(SrcQuery.Fields).AsQuad[l] := QuadNull;
                TSQLDA(SrcQuery.Fields).IsNull[l] := false;
                ArrayPutSlice(dbhandle, trhandle, PGDSQuad(SrcQuery.Fields.Data.sqlvar[l].SqlData)^, SrcQuery.Fields.ArrayInfos[F].info, SrcQuery.Fields.ArrayData[l], SrcQuery.Fields.ArrayInfos[F].size);
              end;

            try
              CheckDstTransaction;
              DSQLExecute(trhandle, sthandle, 3, SrcQuery.Fields);
              Inc(done);
              if failsafe or (done mod commit_interval = 0) then
              begin
                DstTransaction.Commit;
                Write('    ', done, ' records' + #13);
              end;
            except
              on E: EUIBError do
              begin
                LogError(E, SrcQuery);
                DstTransaction.Commit;
                Inc(FErrorsCount);
              end;
            end;

            SrcQuery.Next;
          end;

          if DstTransaction.InTransaction then
          begin
            try
              DstTransaction.Commit;
              Write('    ', done, ' records' + #13);
            except
              on E: EUIBError do
              begin
                LogError(E, SrcQuery);
//                DstTransaction.RollBack;
                Inc(FErrorsCount);
              end;
            end;
          end;

          WriteLn;

          DSQLFreeStatement(sthandle, DSQL_drop);
        end;
      end;

      SrcQuery.Close(etmCommit);
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

  try
    SrcDatabase.Connected := true
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
    DstDatabase.Connected := true;
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
        DstDatabase.DropDatabase;
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
      metadb.LoadFromDatabase(SrcTransaction);
      meta_charset := metadb.DefaultCharset;
      metadb.Free;
      metadb := TMetaDatabase.Create(nil, -1);
      SrcDatabase.Connected := false;
      SrcDatabase.CharacterSet := meta_charset;
      SrcDatabase.Connected := true;
    end;

    { Lit l'intégralité des métadonnées }
    metadb.OIDDatabases := ALLObjects;
    metadb.LoadFromDatabase(SrcTransaction);

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
      SrcDatabase.Connected := false;
      SrcDatabase.CharacterSet := data_charset;
      SrcDatabase.Connected := true;
    end;

    if not PumpOnly then
    begin
      if PageSize = 0 then
        PageSize := SrcDatabase.InfoPageSize;

      DstDatabase.CharacterSet := data_charset;
      DstDatabase.CreateDatabase(data_charset, PageSize);

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
        ExecuteImmediate(metadb.Roles[i].AsFullDDL);
      end;

      // UDF
      AddLog(#13#10 + 'Create UDF');
      for i := 0 to metadb.UDFSCount - 1 do
      begin
        AddLog('  %s', [metadb.UDFS[i].Name]);
        ExecuteImmediate(metadb.UDFS[i].AsFullDDL);
      end;

      // DOMAINS
      AddLog(#13#10 + 'Create Domains');
      for i := 0 to metadb.DomainsCount - 1 do
      begin
        AddLog('  %s', [metadb.Domains[i].Name]);
        ExecuteImmediate(metadb.Domains[i].AsFullDDL);
      end;

      // GENERATORS
      AddLog(#13#10 + 'Create Generators');
      for i := 0 to metadb.GeneratorsCount - 1 do
      begin
        AddLog('  %s', [metadb.Generators[i].Name]);
        ExecuteImmediate(metadb.Generators[i].AsCreateDLL);
      end;

      // EXEPTIONS
      AddLog(#13#10 + 'Create Exceptions');
      for i := 0 to metadb.ExceptionsCount - 1 do
      begin
        AddLog('  %s', [metadb.Exceptions[i].Name]);
        ExecuteImmediate(metadb.Exceptions[i].AsFullDDL);
      end;

      // EMPTY PROCEDURES
      AddLog(#13#10 + 'Create Empty Procedures');
      for i := 0 to metadb.ProceduresCount - 1 do
      begin
        AddLog('  %s', [metadb.Procedures[i].Name]);
        ExecuteImmediate(metadb.Procedures[i].AsCreateEmptyDDL);
      end;

      // TABLES
      AddLog(#13#10 + 'Create Tables');
      for i := 0 to metadb.TablesCount - 1 do
      begin
        AddLog('  %s', [metadb.Tables[i].Name]);
        ExecuteImmediate(metadb.Tables[i].AsFullDDLNode);
      end;

      // VIEWS
      AddLog(#13#10 + 'Create Views');
      for i := 0 to metadb.ViewsCount - 1 do
      begin
        AddLog('  %s', [metadb.Views[i].Name]);
        ExecuteImmediate(metadb.Views[i].AsFullDDLNode);
      end;
    end;

    // TABLES DATA
    AddLog(#13#10 + 'Pump Data');
    dbhandle := DstDatabase.DbHandle;
    DstTransaction.Commit;
    PumpData(dbhandle, metadb, data_charset, failsafe, CommitInterval, PumpOnly); // Sort Tables by Foreign Keys in case of Data Pump (constraints already exists)

    // GENERATORS VALUES
    AddLog(#13#10 + 'Sync Generators');
    for i := 0 to metadb.GeneratorsCount - 1 do
    begin
      AddLog('  %s', [metadb.Generators[i].Name]);
      ExecuteImmediate(metadb.Generators[i].AsAlterDDL);
    end;

    if not PumpOnly then
    begin
      // UNIQUE
      AddLog(#13#10 + 'Create Unique Indices');
      for i := 0 to metadb.TablesCount - 1 do
      for j := 0 to metadb.Tables[i].UniquesCount - 1 do
      begin
        AddLog('  %s', [metadb.Tables[i].Uniques[j].Name]);
        ExecuteImmediate(metadb.Tables[i].Uniques[j].AsFullDDL);
      end;

      // PRIMARY
      AddLog(#13#10 + 'Create Primary Keys Constraints');
      for i := 0 to metadb.TablesCount - 1 do
      for j := 0 to metadb.Tables[i].PrimaryCount - 1 do
      begin
        AddLog('  %s', [metadb.Tables[i].Primary[j].Name]);
        ExecuteImmediate(metadb.Tables[i].Primary[j].AsFullDDL);
      end;

      // FOREIGN
      AddLog(#13#10 + 'Create Foreign Keys Constraints');
      for i := 0 to metadb.TablesCount - 1 do
      for j := 0 to metadb.Tables[i].ForeignCount - 1 do
      begin
        AddLog('  %s', [metadb.Tables[i].Foreign[j].Name]);
        ExecuteImmediate(metadb.Tables[i].Foreign[j].AsFullDDL);
      end;

      // INDICES
      AddLog(#13#10 + 'Create Indices');
      for i := 0 to metadb.TablesCount - 1 do
      for j := 0 to metadb.Tables[i].IndicesCount - 1 do
      begin
        AddLog('  %s', [metadb.Tables[i].Indices[j].Name]);
        ExecuteImmediate(metadb.Tables[i].Indices[j].AsFullDDL);
      end;

      // CHECKS
      AddLog(#13#10 + 'Create Check Constraints');
      for i := 0 to metadb.TablesCount - 1 do
      for j := 0 to metadb.Tables[i].ChecksCount - 1 do
      begin
        AddLog('  %s', [metadb.Tables[i].Checks[j].Name]);
        ExecuteImmediate(metadb.Tables[i].Checks[j].AsFullDDL);
      end;

      // TABLE TRIGGERS
      AddLog(#13#10 + 'Create Triggers');
      for i := 0 to metadb.TablesCount - 1 do
      for j := 0 to metadb.Tables[i].TriggersCount - 1 do
      begin
        AddLog('  %s', [metadb.Tables[i].Triggers[j].Name]);
        ExecuteImmediate(metadb.Tables[i].Triggers[j].AsFullDDL);
      end;

      // VIEW TRIGGERS
      AddLog(#13#10 + 'Create Views');
      for i := 0 to metadb.ViewsCount - 1 do
      for j := 0 to metadb.Views[i].TriggersCount - 1 do
      begin
        AddLog('  %s', [metadb.Views[i].Triggers[j].Name]);
        ExecuteImmediate(metadb.Views[i].Triggers[j].AsFullDDL);
      end;

      // ALTER PROCEDURES
      AddLog(#13#10 + 'Create Procedures Code');
      for i := 0 to metadb.ProceduresCount - 1 do
      begin
        AddLog('  %s', [metadb.Procedures[i].Name]);
        ExecuteImmediate(metadb.Procedures[i].AsAlterDDL);
      end;

      // GRANTS
      AddLog(#13#10 + 'Grant Roles');
      for i := 0 to metadb.RolesCount - 1 do
      begin
        for j := 0 to metadb.Roles[i].GrantsCount - 1 do
        begin
           AddLog('  %s', [metadb.Roles[i].Grants[j].Name]);
           ExecuteImmediate(metadb.Roles[i].Grants[j].AsFullDDL);
        end;
      end;

      AddLog(#13#10 + 'Grant Tables and Fields');
      for i := 0 to metadb.TablesCount - 1 do
      begin
        for j := 0 to metadb.Tables[i].GrantsCount - 1 do
        begin
          AddLog('  Table %s', [metadb.Tables[i].Grants[j].Name]);
          ExecuteImmediate(metadb.Tables[i].Grants[j].AsFullDDL);
        end;
        for j := 0 to metadb.Tables[i].FieldsGrantsCount - 1 do
        begin
          AddLog('  Field: %s', [metadb.Tables[i].FieldsGrants[j].Name]);
          ExecuteImmediate(metadb.Tables[i].FieldsGrants[j].AsFullDDL);
        end;
      end;

      AddLog(#13#10 + 'Grant Views and Fields');
      for i := 0 to metadb.ViewsCount - 1 do
      begin
        for j := 0 to metadb.Views[i].GrantsCount - 1 do
        begin
          AddLog('  View %s', [metadb.Views[i].Grants[j].Name]);
          ExecuteImmediate(metadb.Views[i].Grants[j].AsFullDDL);
        end;
        for j := 0 to metadb.Views[i].FieldsGrantsCount - 1 do
        begin
          AddLog('  Field %s', [metadb.Views[i].FieldsGrants[j].Name]);
          ExecuteImmediate(metadb.Tables[i].FieldsGrants[j].AsFullDDL);
        end;
      end;

      AddLog(#13#10 + 'Grant Procedures');
      for i := 0 to metadb.ProceduresCount - 1 do
      begin
        for j := 0 to metadb.Procedures[i].GrantsCount - 1 do
        begin
          AddLog('  %s', [metadb.Procedures[i].Grants[j].Name]);
          ExecuteImmediate(metadb.Procedures[i].Grants[j].AsFullDDL);
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
        GO.PrintSyntax;
        WriteLn;
        GO.PrintHelp;
        Exit;
      end;

      if not GO.Validate then
      begin
        WriteLn('Missing parameters on command line:');
        for O in GO.Missing do
          WriteLn(' ', O.ToShortSyntax);
        WriteLn;
        GO.PrintSyntax;
        Exit;
      end;

      if GO.Flag['u'] and (GO.Flag['su'] or GO.Flag['tu']) then
      begin
        WriteLn('Conflict between parameters on command line:');
        WriteLn(' ', 'Flags -tu and -su cannot be used with -u');
        WriteLn;
        GO.PrintSyntax;
        Exit;
      end;

      if GO.Flag['p'] and (GO.Flag['sp'] or GO.Flag['tp']) then
      begin
        WriteLn('Conflict between parameters on command line:');
        WriteLn(' ', 'Flags -tp and -sp cannot be used with -p');
        WriteLn;
        GO.PrintSyntax;
        Exit;
      end;

      if GO.Flag['po'] and GO.Flag['ps'] then
      begin
        WriteLn('Useless flag on command line:');
        WriteLn(' ', 'The flag -ps (Page Size) will be ignored if -po (Pump Only Mode) is specified');
        WriteLn;
      end;

      if GO.Flag['f'] and GO.Flag['ci'] then
      begin
        WriteLn('Useless flag on command line:');
        WriteLn(' ', 'The flag -ci (Commit Interval) will be ignored if -f (Failsafe Mode) is specified');
        WriteLn;
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
        WriteLn('Error on command line ' + E.Message);
        GO.PrintSyntax;
        Exit;
      end;
    end;
  finally
    GO.Free;
  end;

  try
    Clone(src, tgt, page_size, data_charset, meta_charset, verbose, pump_only, failsafe, commit_interval, dump_file);
  except
    on E: Exception do
    begin
      WriteLn(E.Message);
      Read;
    end;
  end;
end.
