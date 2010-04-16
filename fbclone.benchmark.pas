unit fbclone.benchmark;

interface

uses
  Windows, SysUtils;

type
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
    function FormatRPS(d: Extended; c: Cardinal): String; {$IFDEF RELEASE}inline;{$ENDIF}
  public
    duration: TDuration;
    last_duration: TDuration;
    count: Cardinal;
    last_count: Cardinal;
    procedure Start; {$IFDEF RELEASE}inline;{$ENDIF}
    function Stop: Extended; {$IFDEF RELEASE}inline;{$ENDIF}
    procedure Reset;
    function LastRPSToString: String;
    function RPSToString: String;
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
    deleting: TPerformanceCounter;
    blobs: TPerformanceCounter;
    arrays: TPerformanceCounter;
    function LastToString(indent: Integer): String;
    function GlobalToString: String;
    procedure Reset;
    function TotalWritingTime: Extended;
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
    target_metadata: TPerformanceCounter;
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

implementation

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
  deleting.Reset;
  writing.Reset;
  blobs.Reset;
  arrays.Reset;
end;

function TPerformanceStats.TotalWritingTime: Extended;
begin
  Result := transaction_setup.duration.value
          + prepare.duration.value
          + blobs.duration.value
          + arrays.duration.value
          + writing.duration.value
          + transaction_commit.duration.value;
end;

function TPerformanceStats.LastToString(indent: Integer): String;
var
  IndentString: string;
begin
  IndentString := StringOfChar(' ', indent);
  Result :=
    Format(
      IndentString + 'Transaction Setup    [%9d] %s %s' + #13#10 +
      IndentString + 'Transaction Commit   [%9d] %s %s' + #13#10 +
      IndentString + 'Statement Prepare    [%9d] %s %s' + #13#10 +
      IndentString + 'Statement Drop       [%9d] %s %s' + #13#10 +
      IndentString + 'Source Prepare       [%9d] %s %s' + #13#10 +
      IndentString + 'Source Data Read     [%9d] %s %s' + #13#10 +
      IndentString + 'Source Data Commit   [%9d] %s %s' + #13#10 +
      IndentString + 'Target Data Deletion [%9d] %s %s' + #13#10 +
      IndentString + 'Target Data Write    [%9d] %s %s' + #13#10 +
      IndentString + 'Target Blob Create   [%9d] %s %s' + #13#10 +
      IndentString + 'Target Arrays Create [%9d] %s %s',
      [
        transaction_setup.last_count,  transaction_setup.last_duration.ToString,  transaction_setup.LastRPSToString,
        transaction_commit.last_count, transaction_commit.last_duration.ToString, transaction_commit.LastRPSToString,
        prepare.last_count,            prepare.last_duration.ToString,            prepare.LastRPSToString,
        drop.last_count,               drop.last_duration.ToString,               drop.LastRPSToString,
        reading_prepare.last_count,    reading_prepare.last_duration.ToString,    reading_prepare.LastRPSToString,
        reading.last_count,            reading.last_duration.ToString,            reading.LastRPSToString,
        reading_commit.last_count,     reading_commit.last_duration.ToString,     reading_commit.LastRPSToString,
        deleting.last_count,           deleting.last_duration.ToString,           deleting.LastRPSToString,
        writing.last_count,            writing.last_duration.ToString,            writing.LastRPSToString,
        blobs.last_count,              blobs.last_duration.ToString,              blobs.LastRPSToString,
        arrays.last_count,             arrays.last_duration.ToString,             arrays.LastRPSToString
      ]);
end;

function TPerformanceStats.GlobalToString: String;
begin
  Result :=
    Format(
      '  Transaction Setup    [%9d] %s %s' + #13#10 +
      '  Transaction Commit   [%9d] %s %s' + #13#10 +
      '  Statement Prepare    [%9d] %s %s' + #13#10 +
      '  Statement Drop       [%9d] %s %s' + #13#10 +
      '  Source Prepare       [%9d] %s %s' + #13#10 +
      '  Source Data Read     [%9d] %s %s' + #13#10 +
      '  Source Data Commit   [%9d] %s %s' + #13#10 +
      '  Target Data Deletion [%9d] %s %s' + #13#10 +
      '  Target Data Write    [%9d] %s %s' + #13#10 +
      '  Target Blob Create   [%9d] %s %s' + #13#10 +
      '  Target Arrays Create [%9d] %s %s',
      [
        transaction_setup.count,  transaction_setup.duration.ToString,  transaction_setup.RPSToString,
        transaction_commit.count, transaction_commit.duration.ToString, transaction_commit.RPSToString,
        prepare.count,            prepare.duration.ToString,            prepare.RPSToString,
        drop.count,               drop.duration.ToString,               drop.RPSToString,
        reading_prepare.count,    reading_prepare.duration.ToString,    reading_prepare.RPSToString,
        reading.count,            reading.duration.ToString,            reading.RPSToString,
        reading_commit.count,     reading_commit.duration.ToString,     reading_commit.RPSToString,
        deleting.count,           deleting.duration.ToString,           deleting.RPSToString,
        writing.count,            writing.duration.ToString,            writing.RPSToString,
        blobs.count,              blobs.duration.ToString,              blobs.RPSToString,
        arrays.count,             arrays.duration.ToString,             arrays.RPSToString
      ]);
end;

{ TPerformanceCounter }

function TPerformanceCounter.FormatRPS(d: Extended; c: Cardinal): String;
begin
  if d <> 0 then
    Result := Format('%12.1f rps', [c / d])
  else
    Result := '           - rps';
end;

function TPerformanceCounter.RPSToString: String;
begin
  Result := FormatRPS(duration.value, count);
end;

function TPerformanceCounter.LastRPSToString: String;
begin
  Result := FormatRPS(last_duration.value, last_count);
end;

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

  if (h > 0) or (m > 0) then
    Result := Format('%.2d:%.2d:%.2d%s h ', [h, m, s, mss])
  else if (s > 0) then
    Result := Format('      %2d%s s ', [s, mss])
  else if ms > 0.00001 then
    Result := Format('%13.4f ms', [ms * 1000])
  else
    Result := Format('%13d ms', [0]);
end;

end.
