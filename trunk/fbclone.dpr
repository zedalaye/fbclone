program fbclone;

{$APPTYPE CONSOLE}

uses
  Windows,
  SysUtils,
  Generics.Collections,
  UIB,
  UIBLib,
  UIBase,
  UIBMetadata,
  UIBConst,
  CxGetOpts;

type
  TLogCategory = (lcInfo, lcAction, lcResultat);
  TScriptResult = (srNothingDone, srEchec, srSucces, srAnnule);

  TDatabase = record
    ConnectionString: string;
    Username: string;
    Password: string;
  end;

procedure TraceAction(const DB: string; const Action: String; Categorie: TLogCategory; Resultat: TScriptResult);
begin
  WriteLn(Action);
end;

function Clone(const Source, Target: TDatabase; const DataCharset, MetaCharset: string; Verbose: Boolean): Boolean;
var
  SrcDatabase, DstDatabase: TUIBDatabase;
  SrcTransaction, DstTransaction: TUIBTransaction;
  SrcQuery: TUIBQuery;
  FErrorsCount: Integer;

  procedure AddLog(const What: string; Categorie: TLogCategory = lcInfo; Resultat: TScriptResult = srSucces); overload;
  begin
    if Verbose then
      TraceAction(Source.ConnectionString, What, Categorie, Resultat);
  end;

  procedure AddLog(const FmtStr: String; const Args: array of const; Categorie: TLogCategory = lcInfo; Resultat: TScriptResult = srSucces); overload;
  begin
    AddLog(Format(FmtStr, Args), Categorie, Resultat);
  end;

  procedure PumpData(dbhandle: IscDbHandle; mdb: TMetaDataBase;
    failsafe: Boolean; sorttables: Boolean);
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

  begin
    DstTransaction.StartTransaction;
    trhandle := DstTransaction.TrHandle;

    for T := 0 to TablesCount - 1 do
    try
      Table := Tables(T);
      AddLog('Filling Table: %s', [Table.Name]);
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
          sthandle := nil;
          DSQLAllocateStatement(dbhandle, sthandle);

          DSQLPrepare(dbhandle, trhandle, sthandle,  MBUEncode(sql, CharacterSetCP[csISO8859_1]), 3, nil);

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
                l := SrcQuery.Fields.ArrayInfos[F].index;
                TSQLDA(SrcQuery.Fields).AsQuad[l] := QuadNull;
                TSQLDA(SrcQuery.Fields).IsNull[l] := false;
                ArrayPutSlice(dbhandle, trhandle, PGDSQuad(SrcQuery.Fields.Data.sqlvar[l].SqlData)^, SrcQuery.Fields.ArrayInfos[F].info, SrcQuery.Fields.ArrayData[l], SrcQuery.Fields.ArrayInfos[F].size);
              end;

            try
              DSQLExecute(trhandle, sthandle, 3, SrcQuery.Fields);
              Inc(done);
              if failsafe or (done mod 500 = 0) then
                DstTransaction.CommitRetaining;
              if (done mod 10000 = 0) then
                AddLog('Pumped %d records',[done]);
            except
              on E: EUIBError do
              begin
                AddLog('--- failed ---', lcInfo, srEchec);
                AddLog('ErrorCode = %d' + ''#13''#10'' + 'SQLCode = %d', [E.ErrorCode, E.SQLCode], lcResultat, srEchec);
                AddLog(e.Message, lcResultat, srEchec);
                AddLog('--- source fields values ---', lcResultat, srEchec);
                for c := 0 to SrcQuery.Fields.FieldCount - 1 do
                  case SrcQuery.Fields.FieldType[c] of
                    uftBlob, uftBlobId:
                      AddLog('%s = [BLOB]', [SrcQuery.Fields.AliasName[c]], lcResultat, srEchec);
                    uftArray:
                      AddLog('%s = [ARRAY]', [SrcQuery.Fields.AliasName[c]], lcResultat, srEchec)
                    else
                      AddLog('%s = %s', [SrcQuery.Fields.AliasName[c], SrcQuery.Fields.AsString[c]], lcResultat, srEchec);
                  end;
                AddLog('--- rolling back record and continue ---', lcResultat, srEchec);
                DstTransaction.RollBackRetaining;
                Inc(FErrorsCount);
              end;
            end;

            SrcQuery.Next;
          end;
          DSQLFreeStatement(sthandle, DSQL_drop);
        end;
      end;
      SrcQuery.Close(etmStayIn);
    except
      on E: Exception do
      begin
        AddLog('--- failed ---', lcResultat, srEchec);
        AddLog(e.Message, lcResultat, srEchec);
        AddLog('--------------', lcResultat, srEchec);
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
        AddLog('--- failed ---', lcResultat, srEchec);
        AddLog(sql, lcResultat, srEchec);
        AddLog('---  exception  ---', lcResultat, srEchec);
        AddLog(e.Message, lcResultat, srEchec);
        AddLog('--------------', lcResultat, srEchec);
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

  try
    SrcDatabase.Connected := true
  except
    on E: EUIBError do
    begin
      WriteLn('Impossible de se connecter à la base de données source ' + Source.ConnectionString + #13#10 +
              E.Message);
      Exit;
    end;
  end;

  DstDatabase := TUIBDataBase.Create(nil);
  DstDatabase.DatabaseName := Target.ConnectionString;
  DstDatabase.UserName := Target.Username;
  DstDatabase.PassWord := Target.Password;
  DstDatabase.CharacterSet := meta_charset;

  try
    DstDatabase.Connected := true;
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
  except
    { si on ne peut pas se connecter c'est que la base n'existe pas ! tout baigne }
  end;

  metadb := TMetaDataBase.Create(nil,-1);

  SrcTransaction := TUIBTransaction.Create(nil);
  SrcQuery := TUIBQuery.Create(nil);

  DstTransaction := TUIBTransaction.Create(nil);
  try
    SrcTransaction.DataBase := SrcDatabase;
    SrcQuery.Transaction := SrcTransaction;
    SrcQuery.FetchBlobs := true;

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
    metadb.OIDDatabases := ALLObjects - [OIDDBCharset];
    metadb.LoadFromDatabase(SrcTransaction);

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

    AddLog('Create database (page_size %d)', [SrcDatabase.InfoPageSize]);
    DstDatabase.CharacterSet := data_charset;
    DstDatabase.CreateDatabase(SrcDatabase.InfoPageSize);

    // ROLES
    for i := 0 to metadb.RolesCount - 1 do
    begin
      AddLog('Create role: %s', [metadb.Roles[i].Name]);
      ExecuteImmediate(metadb.Roles[i].AsFullDDL);
    end;

    // UDF
    for i := 0 to metadb.UDFSCount - 1 do
    begin
      AddLog('Create UDF: %s', [metadb.UDFS[i].Name]);
      ExecuteImmediate(metadb.UDFS[i].AsFullDDL);
    end;

    // DOMAINS
    for i := 0 to metadb.DomainsCount - 1 do
    begin
      AddLog('Create Domain: %s', [metadb.Domains[i].Name]);
      ExecuteImmediate(metadb.Domains[i].AsFullDDL);
    end;

    // GENERATORS
    for i := 0 to metadb.GeneratorsCount - 1 do
    begin
      AddLog('Create Generator: %s', [metadb.Generators[i].Name]);
      ExecuteImmediate(metadb.Generators[i].AsCreateDLL);
      ExecuteImmediate(metadb.Generators[i].AsAlterDDL);
    end;

    // EXEPTIONS
    for i := 0 to metadb.ExceptionsCount - 1 do
    begin
      AddLog('Create Exception: %s', [metadb.Exceptions[i].Name]);
      ExecuteImmediate(metadb.Exceptions[i].AsFullDDL);
    end;

    // EMPTY PROCEDURES
    for i := 0 to metadb.ProceduresCount - 1 do
    begin
      AddLog('Create Empty Procedure: %s', [metadb.Procedures[i].Name]);
      ExecuteImmediate(metadb.Procedures[i].AsCreateEmptyDDL);
    end;

    // TABLES
    for i := 0 to metadb.TablesCount - 1 do
    begin
      AddLog('Create Table: %s', [metadb.Tables[i].Name]);
      ExecuteImmediate(metadb.Tables[i].AsFullDDLNode);
    end;

    // VIEWS
    for i := 0 to metadb.ViewsCount - 1 do
    begin
      AddLog('Create View: %s', [metadb.Views[i].Name]);
      ExecuteImmediate(metadb.Views[i].AsFullDDLNode);
    end;

    // TABLES DATA
    dbhandle := DstDatabase.DbHandle;
    DstTransaction.Commit;
    PumpData(dbhandle, metadb, false, false);

    // UNIQUE
    for i := 0 to metadb.TablesCount - 1 do
    for j := 0 to metadb.Tables[i].UniquesCount - 1 do
    begin
      AddLog('Create Unique: %s', [metadb.Tables[i].Uniques[j].Name]);
      ExecuteImmediate(metadb.Tables[i].Uniques[j].AsFullDDL);
    end;

    // PRIMARY
    for i := 0 to metadb.TablesCount - 1 do
    for j := 0 to metadb.Tables[i].PrimaryCount - 1 do
    begin
      AddLog('Create Primary: %s', [metadb.Tables[i].Primary[j].Name]);
      ExecuteImmediate(metadb.Tables[i].Primary[j].AsFullDDL);
    end;

    // FOREIGN
    for i := 0 to metadb.TablesCount - 1 do
    for j := 0 to metadb.Tables[i].ForeignCount - 1 do
    begin
      AddLog('Create Foreign: %s', [metadb.Tables[i].Foreign[j].Name]);
      ExecuteImmediate(metadb.Tables[i].Foreign[j].AsFullDDL);
    end;

    // INDICES
    for i := 0 to metadb.TablesCount - 1 do
    for j := 0 to metadb.Tables[i].IndicesCount - 1 do
    begin
      AddLog('Create Indice: %s', [metadb.Tables[i].Indices[j].Name]);
      ExecuteImmediate(metadb.Tables[i].Indices[j].AsFullDDL);
    end;

    // CHECKS
    for i := 0 to metadb.TablesCount - 1 do
    for j := 0 to metadb.Tables[i].ChecksCount - 1 do
    begin
      AddLog('Create Check: %s', [metadb.Tables[i].Checks[j].Name]);
      ExecuteImmediate(metadb.Tables[i].Checks[j].AsFullDDL);
    end;

    // TABLE TRIGGERS
    for i := 0 to metadb.TablesCount - 1 do
    for j := 0 to metadb.Tables[i].TriggersCount - 1 do
    begin
      AddLog('Create Trigger: %s', [metadb.Tables[i].Triggers[j].Name]);
      ExecuteImmediate(metadb.Tables[i].Triggers[j].AsFullDDL);
    end;

    // VIEW TRIGGERS
    for i := 0 to metadb.ViewsCount - 1 do
    for j := 0 to metadb.Views[i].TriggersCount - 1 do
    begin
      AddLog('Create Trigger: %s', [metadb.Views[i].Triggers[j].Name]);
      ExecuteImmediate(metadb.Views[i].Triggers[j].AsFullDDL);
    end;

    // ALTER PROCEDURES
    for i := 0 to metadb.ProceduresCount - 1 do
    begin
      AddLog('Alter Procedure: %s', [metadb.Procedures[i].Name]);
      ExecuteImmediate(metadb.Procedures[i].AsAlterDDL);
    end;

    // GRANTS
    for i := 0 to metadb.RolesCount - 1 do
    begin
      for j := 0 to metadb.Roles[i].GrantsCount - 1 do
      begin
         AddLog('Grant To Role: %s', [metadb.Roles[i].Grants[j].Name]);
         ExecuteImmediate(metadb.Roles[i].Grants[j].AsFullDDL);
      end;
    end;

    for i := 0 to metadb.TablesCount - 1 do
    begin
      for j := 0 to metadb.Tables[i].GrantsCount - 1 do
      begin
        AddLog('Grant To Table: %s', [metadb.Tables[i].Grants[j].Name]);
        ExecuteImmediate(metadb.Tables[i].Grants[j].AsFullDDL);
      end;
      for j := 0 to metadb.Tables[i].FieldsGrantsCount - 1 do
      begin
        AddLog('Grant To TableField: %s', [metadb.Tables[i].FieldsGrants[j].Name]);
        ExecuteImmediate(metadb.Tables[i].FieldsGrants[j].AsFullDDL);
      end;
    end;

    for i := 0 to metadb.ViewsCount - 1 do
    begin
      for j := 0 to metadb.Views[i].GrantsCount - 1 do
      begin
        AddLog('Grant To View: %s', [metadb.Views[i].Grants[j].Name]);
        ExecuteImmediate(metadb.Views[i].Grants[j].AsFullDDL);
      end;
      for j := 0 to metadb.Views[i].FieldsGrantsCount - 1 do
      begin
        AddLog('Grant To ViewField: %s', [metadb.Views[i].FieldsGrants[j].Name]);
        ExecuteImmediate(metadb.Tables[i].FieldsGrants[j].AsFullDDL);
      end;
    end;

    for i := 0 to metadb.ProceduresCount - 1 do
    begin
      for j := 0 to metadb.Procedures[i].GrantsCount - 1 do
      begin
        AddLog('Grant To Procedure: %s', [metadb.Procedures[i].Grants[j].Name]);
        ExecuteImmediate(metadb.Procedures[i].Grants[j].AsFullDDL);
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
      AddLog('--- %d error(s) ! ---', [FErrorsCount], lcResultat, srEchec);
      Result := False;
    end;
  end;
end;

var
  GO: TGetOpt;
  O: POption;
  P: TPair<POption, string>;
  src, tgt: TDatabase;
  data_charset, meta_charset: string;
  verbose: Boolean;
begin
  verbose := false;
  meta_charset := '';

  GO := TGetOpt.Create;
  try
    try
      GO.RegisterFlag('h', 'help', '', 'Affiche la description de tous les paramètres supportés par l''application', false);
      GO.RegisterFlag('v', 'verbose', '', 'Affiche les détails de l''avancement du clonage', false);

      GO.RegisterSwitch('s',  'source',          'database', 'Chaîne de connexion à la base de données source', true);
      GO.RegisterSwitch('su', 'source-username', 'username', 'Login de connexion à la base de données source', true);
      GO.RegisterSwitch('sp', 'source-password', 'password', 'Mot de passe de connexion à la base de données source', true);

      GO.RegisterSwitch('t',  'target',          'database', 'Chaîne de connexion à la base de données cible', true);
      GO.RegisterSwitch('tu', 'target-username', 'username', 'Login de connexion à la base de données cible', true);
      GO.RegisterSwitch('tp', 'target-password', 'password', 'Mot de passe de connexion à la base de données cible', true);

      GO.RegisterSwitch('c', 'charset',  'charset',  'Jeu de caractères à utiliser pour la copie des données', false);
      GO.RegisterSwitch('mc', 'metadata-charset', 'charset', 'Jeu de caractères à utiliser pour la connexion aux métadonnées de la base de données source', false);

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
        WriteLn('Il manque des arguments sur la ligne de commande :');
        for O in GO.Missing do
          WriteLn(' ', O.ToShortSyntax);
        WriteLn;
        GO.PrintSyntax;
        Exit;
      end;

      for P in GO.Parameters do
      begin
        if P.Key^.Short = 's' then
          src.ConnectionString := P.Value
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
          verbose := true;
      end;

    except
      on E: Exception do
      begin
        WriteLn('Erreur sur la ligne de commande ' + E.Message);
        GO.PrintSyntax;
        Exit;
      end;
    end;
  finally
    GO.Free;
  end;

  try
    Clone(src, tgt, data_charset, meta_charset, verbose);
  except
    on E: Exception do
    begin
      WriteLn(E.Message);
      Read;
    end;
  end;
end.
