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
 * The Original Code was created by Pierre Yager.
 *)

unit console.getopts;

interface

uses
  Windows, SysUtils, Generics.Collections;

type
  EGetOptError = class(Exception);
  EGetOptErrorState = class(EGetOptError);

  TOptionType = (oUnknown, oFlag, oSwitch, oParameter);

  POption = ^TOption;
  TOption = record
    Short: string;
    Long: string;
    ShortDescription: string;
    Description: string;
    OptionType: TOptionType;
    Mandatory: Boolean;
    function ToShortSyntax: string;
    function ToLongSyntax: string;
  end;

  TParameter = record
    Name: string;
    Value: string;
  end;

  TGetOpt = class
  private

    type
      TState = class
      protected
        FOpt: TGetOpt;
        FContext: POption;
      public
        constructor Create(Opt: TGetOpt);
        procedure Initialize(Context: POption);
        procedure ParseOption(const Value: POption); virtual; abstract;
        procedure ParseText(const Value: string); virtual; abstract;
      end;

      TNoWhereState = class(TState)
      public
        procedure ParseOption(const Value: POption); override;
        procedure ParseText(const Value: string); override;
      end;

      TSwitchValueState = class(TState)
      public
        procedure ParseOption(const Value: POption); override;
        procedure ParseText(const Value: string); override;
      end;

      TStates = (sNoWhere, sSwitchValue);

  private var
    FOptions: TList<POption>;
    FParameters: TDictionary<POption, string>;
    FMissing: TList<POption>;
    FOption: POption;
    FNullOption: POption;
    FState : TState;
    FStates: array[TStates] of TState;

    function CreateOption(const Short, Long, ShortDescription, Description: string; OptionType: TOptionType; Mandatory: Boolean): POption;
    function FindOption(const Value: string): POption;
    function FindNextParameter: POption;
    procedure ChangeState(const State: TStates);
    function GetFlag(const Short: string): Boolean;
  public
    constructor Create;
    destructor Destroy; override;

    procedure RegisterFlag(const Short, Long, ShortDescription, Description: string; Mandatory: Boolean);
    procedure RegisterSwitch(const Short, Long, ShortDescription, Description: string; Mandatory: Boolean);
    procedure RegisterParameter(const ShortDescription, Description: string);

    procedure Parse;
    function Validate: Boolean;

    function PrintSyntax: String;
    function PrintHelp: string;

    property Parameters: TDictionary<POption, string> read FParameters;
    property Missing: TList<POption> read FMissing;
    property Flag[const Short: string]: Boolean read GetFlag;
  end;

implementation

{ TGetOpt }

constructor TGetOpt.Create;
begin
  inherited;

  New(FNullOption);
  FNullOption^.OptionType := oUnknown;

  FStates[sNoWhere] := TNoWhereState.Create(Self);
  FStates[sSwitchValue] := TSwitchValueState.Create(Self);

  FState := FStates[sNoWhere];

  FOptions := TList<POption>.Create;
  FMissing := TList<POption>.Create;
  FParameters := TDictionary<POption, string>.Create;
end;

destructor TGetOpt.Destroy;
var
  O: POption;
begin
  FParameters.Free;
  for O in FOptions do
    Dispose(O);
  FOptions.Free;
  Dispose(FNullOption);
  inherited;
end;

procedure TGetOpt.ChangeState(const State: TStates);
begin
  FState := FStates[State];
  FState.Initialize(FOption);
end;

function TGetOpt.FindOption(const Value: string): POption;
var
  O: POption;
begin
  Result := FNullOption;
  for O in FOptions do
  begin
    if (O^.Short = Value) or (O^.Long = Value) then
    begin
      Result := O;
      Break;
    end;
  end;
end;

function TGetOpt.GetFlag(const Short: string): Boolean;
var
  O: POption;
begin
  Result := false;
  for O in FParameters.Keys do
  begin
    if (O^.OptionType = oFlag) and (O^.Short = Short) then
    begin
      Result := true;
      Break;
    end;
  end;
end;

function TGetOpt.FindNextParameter: POption;
var
  O: POption;
begin
  Result := FNullOption;
  for O in FOptions do
  begin
    if O^.OptionType = oParameter then
    begin
      if not FParameters.ContainsKey(O) then
      begin
        Result := O;
        Break;
      end;
    end;
  end;
end;

function TGetOpt.CreateOption(const Short, Long, ShortDescription,
  Description: string; OptionType: TOptionType; Mandatory: Boolean): POption;
begin
  New(Result);
  Result^.Short := Short;
  Result^.Long := Long;
  Result^.ShortDescription := ShortDescription;
  Result^.Description := Description;
  Result^.Mandatory := Mandatory;
  Result^.OptionType := OptionType;
end;

procedure TGetOpt.RegisterFlag(const Short, Long, ShortDescription,
  Description: string; Mandatory: Boolean);
var
  O: POption;
begin
  O := CreateOption(Short, Long, ShortDescription, Description, oFlag, Mandatory);
  FOptions.Add(O);
end;

procedure TGetOpt.RegisterParameter(const ShortDescription, Description: string);
var
  O: POption;
begin
  O := CreateOption('', '', ShortDescription, Description, oParameter, true);
  FOptions.Add(O);
end;

procedure TGetOpt.RegisterSwitch(const Short, Long, ShortDescription,
  Description: string; Mandatory: Boolean);
var
  O: POption;
begin
  O := CreateOption(Short, Long, ShortDescription, Description, oSwitch, Mandatory);
  FOptions.Add(O);
end;

procedure TGetOpt.Parse;
var
  I: Integer;
  V: string;
begin
  FOption := FNullOption;

  I := 1;
  while I <= ParamCount do
  begin
    V := ParamStr(I);

    if CharInSet(V[1], ['-', '/']) then
    begin
      V := Copy(V, 2, MaxInt);
      FOption := FindOption(V);
      if FOption.OptionType = oUnknown then
        raise EGetOptError.Create('Option not supported: ' + ParamStr(I));
      FState.ParseOption(FOption);
    end
    else
      FState.ParseText(V);

    Inc(I);
  end;
end;

function TGetOpt.PrintHelp: String;
var
  S: string;
  O: POption;
begin
  S := 'Arguments allowed for this application: ';
  for O in FOptions do
    S := S + #13#10 + O^.ToLongSyntax;
  Result := S;
end;

function TGetOpt.PrintSyntax: String;
var
  S: string;
  O: POption;
begin
  S := 'Syntax: ' +  ExtractFileName(ParamStr(0));
  for O in FOptions do
    S := S + O^.ToShortSyntax;
  Result := S;
end;

function TGetOpt.Validate: Boolean;
var
  O: POption;
begin
  FMissing.Clear;
  for O in FOptions do
  begin
    if O.Mandatory then
      if not FParameters.ContainsKey(O) then
        FMissing.Add(O);
  end;
  Result := FMissing.Count = 0;
end;

{ TGetOpt.TState }

constructor TGetOpt.TState.Create(Opt: TGetOpt);
begin
  inherited Create;
  FOpt := Opt;
end;

procedure TGetOpt.TState.Initialize(Context: POption);
begin
  FContext := Context;
end;

{ TGetOpt.TNoWhereState }

procedure TGetOpt.TNoWhereState.ParseOption(const Value: POption);
begin
  if Value^.OptionType = oFlag then
    FOpt.FParameters.Add(Value, '')
  else if Value^.OptionType = oSwitch then
    FOpt.ChangeState(sSwitchValue)
  else if Value^.OptionType = oParameter then
    raise EGetOptErrorState.Create('Impossible');
end;

procedure TGetOpt.TNoWhereState.ParseText(const Value: string);
var
  O: POption;
begin
  O := FOpt.FindNextParameter;
  if O^.OptionType = oUnknown then
    raise EGetOptError.Create('Parameter not allowed: ' + Value)
  else
    FOpt.FParameters.Add(O, Value);
end;

{ TGetOpt.TSwitchValueState }

procedure TGetOpt.TSwitchValueState.ParseOption(const Value: POption);
begin
  raise EGetOptError.Create('You must give a value for parameter -' + FContext^.Short);
end;

procedure TGetOpt.TSwitchValueState.ParseText(const Value: string);
begin
  FOpt.FParameters.Add(FContext, Value);
  FOpt.ChangeState(sNoWhere);
end;

{ TOption }

function TOption.ToShortSyntax: string;
var
  S, L: string;
begin
  Result := ' ';
  if not Mandatory then
    Result := Result + '[';

  if OptionType <> oParameter then
  begin
    if Short <> '' then
      S := '-' + Short;
    if Long <> '' then
      L := '-' + Long;

    if (Short <> '') then
      Result := Result + S
    else
      Result := Result + L;
  end;

  if ShortDescription <> '' then
    Result := Result + ' <' + ShortDescription + '>';

  if not Mandatory then
    Result := Result + ']';
end;

function TOption.ToLongSyntax: string;
var
  S, L: string;
begin
  Result := ' ';

  if OptionType <> oParameter then
  begin
    if Short <> '' then
      S := '-' + Short;
    if Long <> '' then
      L := '-' + Long;

    if (Short <> '') and (Long <> '') then
      Result := Result + S + ', ' + L
    else
      Result := Result + S + L;
  end;

  if not Mandatory then
    Result := Result + #9 + '[optional]';

  Result := Result + #13#10 + #9 + Description + #13#10;
end;

end.
