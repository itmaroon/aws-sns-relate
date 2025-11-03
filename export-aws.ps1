<# ===== export-from-former2-lite.ps1 =====
 Former2 テンプレを設計図に、以下のみ取得します：
  - lambda/*.zip（関数コード）
  - layers/*.zip（レイヤーコード）
  - stepfunctions/*.asl.json（定義のみ）
#>

# ================== 設定 ==================
$Region        = "ap-northeast-1"
$TemplatePath  = ".\former2.yaml"
$OutRoot       = Join-Path $PWD ("export-{0}" -f (Get-Date -Format "yyyyMMdd-HHmmss"))

# 必要なら関数名のプレフィックス補完
$LambdaNamePrefixes = @("lambda_", "lambda-", "ddb_helpers")

# ================== 環境（安全側） ==================
$Env:AWS_PAGER = ""
if (-not $Env:AWS_DEFAULT_OUTPUT) { $Env:AWS_DEFAULT_OUTPUT = "json" }

# ================== 出力フォルダ ==================
$null = New-Item -ItemType Directory -Force -Path `
  $OutRoot, `
  (Join-Path $OutRoot "lambda"), `
  (Join-Path $OutRoot "layers"), `
  (Join-Path $OutRoot "stepfunctions")

# アカウントID（Layer ARN 組立て等で使用）
$AccountId = aws sts get-caller-identity --query 'Account' --output text

# ================== Former2 テンプレ読み込み ==================
$TemplateRaw = Get-Content $TemplatePath -Raw
$TemplateObj = $null
try { $TemplateObj = ConvertFrom-Yaml -Yaml $TemplateRaw } catch { $TemplateObj = $null }

# Lambda 関数名候補の抽出
$FnNamesFromYaml = @()
if ($TemplateObj) {
  foreach ($p in $TemplateObj.Resources.PSObject.Properties) {
    if ($p.Value.Type -eq 'AWS::Lambda::Function') {
      $fnName = $p.Value.Properties.FunctionName
      if ($fnName -is [string] -and $fnName) { $FnNamesFromYaml += $fnName }
    }
  }
} else {
  $FnNamesFromYaml = [regex]::Matches($TemplateRaw, 'FunctionName:\s*"?([A-Za-z0-9\-_]+)"?') `
    | ForEach-Object { $_.Groups[1].Value } | Select-Object -Unique
}

# Layer 名:Version（例: layer:ffmpeg-amd64:1）
$LayerNameVers = [regex]::Matches($TemplateRaw, 'layer:([A-Za-z0-9\-_]+):([0-9]+)') `
  | ForEach-Object { [PSCustomObject]@{ Name=$_.Groups[1].Value; Version=[int]$_.Groups[2].Value } } `
  | Sort-Object Name -Unique

# テンプレ中にフルARNで書かれている Layer（公開/他アカウント対応）
$LayerArnsInTemplate = [regex]::Matches($TemplateRaw, 'arn:aws:lambda:[^"\s]+:layer:[^:"\s]+:[0-9]+') `
  | ForEach-Object { $_.Value } | Select-Object -Unique

# Step Functions 名（StateMachineName）
$SfnNames = @()
if ($TemplateObj) {
  foreach ($p in $TemplateObj.Resources.PSObject.Properties) {
    if ($p.Value.Type -eq 'AWS::StepFunctions::StateMachine') {
      $name = $p.Value.Properties.StateMachineName
      if ($name) { $SfnNames += "$name" }
    }
  }
} else {
  $SfnNames = [regex]::Matches($TemplateRaw, 'StateMachineName:\s*"([^"]+)"') `
    | ForEach-Object { $_.Groups[1].Value } | Select-Object -Unique
}

# ================== 参考表示 ==================
@"
[INFO] Targets
  Lambda(FunctionName) : $($FnNamesFromYaml -join ', ')
  Layers(Name:Ver/ARN) : $((($LayerNameVers | ForEach-Object { "{0}:{1}" -f $_.Name, $_.Version }) + $LayerArnsInTemplate) -join ', ')
  StepFunctions        : $($SfnNames -join ', ')
"@ | Out-Host

# ================== Lambda（コードZIPのみ） ==================
# テンプレに無いが命名規則で拾いたい場合の補完（任意）
$AllFns = (aws lambda list-functions --region $Region --query 'Functions[].FunctionName' --output text).Split() | Where-Object { $_ }
$FnCandidates = New-Object System.Collections.Generic.HashSet[string]
$FnNamesFromYaml | ForEach-Object { if ($_){ [void]$FnCandidates.Add($_) } }
foreach ($n in $AllFns) {
  foreach ($pfx in $LambdaNamePrefixes) {
    if ($n -like ($pfx + "*")) { [void]$FnCandidates.Add($n) }
  }
}
$FnTargets = @($FnCandidates) | Sort-Object

foreach ($fn in $FnTargets) {
  try {
    Write-Host ("== Lambda ZIP: {0}" -f $fn)
    $url = aws lambda get-function `
      --function-name $fn `
      --region $Region `
      --query 'Code.Location' `
      --output text
    if (-not $url) { throw "URL not found" }
    $zipOut = Join-Path (Join-Path $OutRoot "lambda") ("{0}.zip" -f $fn)
    Invoke-WebRequest -Uri $url -OutFile $zipOut
  } catch {
    Write-Warning ("  skip or failed: {0} ({1})" -f $fn, $_.Exception.Message)
  }
}

# ================== Lambda Layers（ZIPのみ） ==================
# 1) フルARN記載のレイヤー
foreach ($arn in $LayerArnsInTemplate) {
  $resolvedArn = $arn `
    -replace '\$\{AWS::Region\}', $Region `
    -replace '\$\{AWS::AccountId\}', $AccountId
  try {
    Write-Host ("== Layer ZIP(ARN): {0}" -f $resolvedArn)
    $res = aws lambda get-layer-version-by-arn --arn $resolvedArn --region $Region | ConvertFrom-Json
    if (-not $res.Content.Location) { throw "No download URL" }
    $name = ($res.LayerVersionArn -split ':')[-2]
    $ver  = ($res.LayerVersionArn -split ':')[-1]
    $lz   = Join-Path (Join-Path $OutRoot "layers") ("{0}-{1}.zip" -f $name, $ver)
    Invoke-WebRequest -Uri $res.Content.Location -OutFile $lz
  } catch {
    Write-Warning ("  layer not accessible: {0} ({1})" -f $resolvedArn, $_.Exception.Message)
  }
}

# 2) Name:Version 指定のレイヤー（同一アカウント前提）
foreach ($lv in $LayerNameVers) {
  $arn = ("arn:aws:lambda:{0}:{1}:layer:{2}:{3}" -f $Region, $AccountId, $lv.Name, $lv.Version)
  try {
    Write-Host ("== Layer ZIP: {0}" -f $arn)
    $res = aws lambda get-layer-version-by-arn --arn $arn --region $Region | ConvertFrom-Json
    $lz  = Join-Path (Join-Path $OutRoot "layers") ("{0}-{1}.zip" -f $lv.Name, $lv.Version)
    Invoke-WebRequest -Uri $res.Content.Location -OutFile $lz
  } catch {
    Write-Warning ("  layer not found or no access: {0} ({1})" -f $arn, $_.Exception.Message)
  }
}

# ================== Step Functions（定義のみ .asl.json） ==================
if ($SfnNames.Count -gt 0) {
  $allSm = aws stepfunctions list-state-machines --region $Region --query 'stateMachines[].{name:name,arn:stateMachineArn}' | ConvertFrom-Json
  foreach ($nm in $SfnNames) {
    $hit = $allSm | Where-Object { $_.name -eq $nm }
    if ($null -eq $hit) { Write-Warning ("StateMachine not found: {0}" -f $nm); continue }
    try {
      Write-Host ("== StepFunctions ASL: {0}" -f $nm)
      # 定義文字列だけ取得してファイルへ
      $def = aws stepfunctions describe-state-machine --state-machine-arn $hit.arn --region $Region --query 'definition' --output text
      $def | Out-File -Encoding utf8 (Join-Path (Join-Path $OutRoot "stepfunctions") ("{0}.asl.json" -f $nm))
    } catch {
      Write-Warning ("  failed to get definition: {0} ({1})" -f $nm, $_.Exception.Message)
    }
  }
}

Write-Host ("`n[OK] Export(Lite) -> {0}" -f $OutRoot)
