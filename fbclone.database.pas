unit fbclone.database;

interface

uses
  Windows, UIB, UIBLib;

type
  TDatabase = record
    LibraryFileName: string;
    ConnectionString: string;
    Username: string;
    Password: string;
    procedure Configure(var Database: TUIBDatabase);
  end;

procedure MapEnvironment(var D: TDatabase);

implementation

{ TDatabase }

procedure TDatabase.Configure(var Database: TUIBDatabase);
begin
  if Self.LibraryFileName <> '' then
    Database.LibraryName := Self.LibraryFileName;
  Database.DatabaseName := Self.ConnectionString;
  Database.UserName := Self.Username;
  Database.PassWord := Self.Password;
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

end.
