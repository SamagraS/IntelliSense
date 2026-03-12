param(
    [string]$WorkspaceRoot = (Get-Location).Path
)

$ErrorActionPreference = 'Stop'
$DataURoot = Join-Path $WorkspaceRoot 'data_u'
$UtcNow = (Get-Date).ToUniversalTime().ToString('o')
$InvariantCulture = [System.Globalization.CultureInfo]::InvariantCulture

function SafeString {
    param($Value)
    if ($null -eq $Value) { return '' }
    return [string]$Value
}

function Set-OrAddProperty {
    param(
        [Parameter(Mandatory = $true)]$Object,
        [Parameter(Mandatory = $true)][string]$Name,
        $Value
    )
    if ($Object.PSObject.Properties.Name -contains $Name) {
        $Object.$Name = $Value
    } else {
        $Object | Add-Member -MemberType NoteProperty -Name $Name -Value $Value
    }
}

function Get-DeterministicNumber {
    param(
        [string]$Text,
        [int]$Mod,
        [int]$Offset = 0
    )
    if ([string]::IsNullOrWhiteSpace($Text)) { return $Offset }
    $value = 7
    foreach ($ch in $Text.ToCharArray()) {
        $value = (($value * 31) + [int][char]$ch) % 2147483647
    }
    return (($value % $Mod) + $Offset)
}

function Get-StateCodeFromGstin {
    param([string]$Gstin)
    $stateMap = @{
        '01' = 'JK'; '02' = 'HP'; '03' = 'PB'; '04' = 'CH'; '05' = 'UT'; '06' = 'HR'; '07' = 'DL';
        '08' = 'RJ'; '09' = 'UP'; '10' = 'BR'; '11' = 'SK'; '12' = 'AR'; '13' = 'NL'; '14' = 'MN';
        '15' = 'MZ'; '16' = 'TR'; '17' = 'ML'; '18' = 'AS'; '19' = 'WB'; '20' = 'JH'; '21' = 'OR';
        '22' = 'CG'; '23' = 'MP'; '24' = 'GJ'; '25' = 'DN'; '26' = 'DD'; '27' = 'MH'; '28' = 'AP';
        '29' = 'KA'; '30' = 'GA'; '31' = 'LD'; '32' = 'KL'; '33' = 'TN'; '34' = 'PY'; '35' = 'AN';
        '36' = 'TS'; '37' = 'AP'; '38' = 'LA'
    }
    if (-not [string]::IsNullOrWhiteSpace($Gstin) -and $Gstin.Length -ge 2) {
        $prefix = $Gstin.Substring(0, 2)
        if ($stateMap.ContainsKey($prefix)) { return $stateMap[$prefix] }
    }
    return 'MH'
}

function Get-ListingYear {
    param([string]$ListingDate)
    if ([string]::IsNullOrWhiteSpace($ListingDate)) { return 2010 }
    $parsed = $null
    if ([datetime]::TryParseExact($ListingDate.Trim(), 'dd-MMM-yyyy', $InvariantCulture, [System.Globalization.DateTimeStyles]::None, [ref]$parsed)) {
        return $parsed.Year
    }
    if ($ListingDate -match '(\d{4})$') { return [int]$Matches[1] }
    return 2010
}

function Normalize-EntityText {
    param([string]$Text)
    if ([string]::IsNullOrWhiteSpace($Text)) { return '' }
    $normalized = $Text.ToUpperInvariant()
    $normalized = $normalized -replace '\b(LIMITED|LTD|PRIVATE|PVT|INDIA|INDIAN|COMPANY|CO|LLP)\b', ' '
    $normalized = $normalized -replace '[^A-Z0-9 ]', ' '
    $normalized = $normalized -replace '\s+', ' '
    return $normalized.Trim()
}

function Clamp {
    param(
        [double]$Value,
        [double]$Min,
        [double]$Max
    )
    return [math]::Min($Max, [math]::Max($Min, $Value))
}

Write-Host 'Loading companies file...'
$CompaniesPath = Join-Path $DataURoot 'structured\companies_financial_scenarios.csv'
$Companies = Import-Csv -Path $CompaniesPath

$BridgeMap = @{}
$CompanyRecords = @()
foreach ($row in $Companies) {
    $companyId = (SafeString $row.company_id).Trim()
    if ([string]::IsNullOrWhiteSpace($companyId)) { continue }

    $caseId = (SafeString $row.case_id).Trim()
    $symbol = (SafeString $row.SYMBOL).Trim().ToUpperInvariant()
    $companyName = (SafeString $row.'NAME OF COMPANY').Trim()
    $gstin = (SafeString $row.gstin).Trim().ToUpperInvariant()
    $isin = (SafeString $row.'ISIN NUMBER').Trim().ToUpperInvariant()

    $numMatch = [regex]::Match($companyId, '\d+')
    if ($numMatch.Success) { $companyNum = [int]$numMatch.Value } else { $companyNum = Get-DeterministicNumber -Text $companyId -Mod 100000 -Offset 1 }

    $stateCode = Get-StateCodeFromGstin -Gstin $gstin
    $listingYear = Get-ListingYear -ListingDate (SafeString $row.' DATE OF LISTING')
    $industryNum = [int](Get-DeterministicNumber -Text "$symbol|$($row.sector)|$($row.project_sector)" -Mod 90000 -Offset 10000)
    $industryCode = $industryNum.ToString('D5')
    $sequenceCode = '{0:D6}' -f $companyNum
    $companyCin = 'L{0}{1}{2}PLC{3}' -f $industryCode, $stateCode, $listingYear, $sequenceCode

    $alphaSource = (($symbol -replace '[^A-Z]', '') + (((SafeString $companyName) -replace '[^A-Za-z]', '').ToUpperInvariant()) + 'XXXXX')
    $panPrefix = $alphaSource.Substring(0, 5)
    $panDigits = '{0:D4}' -f ($companyNum % 10000)
    $panSuffixSource = ((((SafeString $companyName) -replace '[^A-Za-z]', '').ToUpperInvariant()) + 'P')
    $panSuffix = $panSuffixSource.Substring(0, 1)
    $pan = "$panPrefix$panDigits$panSuffix"

    Set-OrAddProperty -Object $row -Name 'company_cin' -Value $companyCin
    Set-OrAddProperty -Object $row -Name 'pan' -Value $pan

    $bridgeRecord = [ordered]@{
        company_id   = $companyId
        case_id      = $caseId
        company_cin  = $companyCin
        gstin        = $gstin
        symbol       = $symbol
        pan          = $pan
        company_name = $companyName
        isin         = $isin
    }

    $BridgeMap[$companyId] = $bridgeRecord
    $CompanyRecords += [pscustomobject]$bridgeRecord
}

Write-Host 'Writing companies + entity_master_bridge...'
$Companies | Export-Csv -Path $CompaniesPath -NoTypeInformation -Encoding UTF8
$BridgePath = Join-Path $DataURoot 'structured\entity_master_bridge.csv'
$CompanyRecords | Export-Csv -Path $BridgePath -NoTypeInformation -Encoding UTF8

Write-Host 'Syncing missing schema files into data_u...'
$syncPairs = @(
    @{ src = 'data\alm\alm_features.csv'; dst = 'data_u\alm\alm_features.csv' },
    @{ src = 'data\structured\borrowing_profile_synthetic.csv'; dst = 'data_u\structured\borrowing_profile_synthetic.csv' },
    @{ src = 'data\structured\portfolio_performance.csv'; dst = 'data_u\structured\portfolio_performance.csv' }
)

foreach ($pair in $syncPairs) {
    $srcPath = Join-Path $WorkspaceRoot $pair.src
    $dstPath = Join-Path $WorkspaceRoot $pair.dst
    if (Test-Path $srcPath) {
        $dstDir = Split-Path -Parent $dstPath
        if (-not (Test-Path $dstDir)) { New-Item -Path $dstDir -ItemType Directory | Out-Null }
        Copy-Item -Path $srcPath -Destination $dstPath -Force
    }
}
Write-Host 'Rebuilding MCA tables...'
$McaDir = Join-Path $DataURoot 'external intelligence\mca'
$McaCompanyPath = Join-Path $McaDir 'mca_company_master.csv'
$McaDirectorsPath = Join-Path $McaDir 'mca_directors.csv'
$DirectorNetworkPath = Join-Path $McaDir 'director_company_network.csv'
$McaChargesPath = Join-Path $McaDir 'mca_charges_registered.csv'
$McaLiveChargesPath = Join-Path $McaDir 'mca_charges_live_only.csv'

$mcaCompanyRows = @()
$mcaDirectorRows = @()
$directorNetworkRows = @()
$mcaChargeRows = @()

$firstNames = @('Ramesh', 'Anita', 'Vivek', 'Priya', 'Sanjay', 'Neha', 'Rahul', 'Kavita', 'Amit', 'Sunita')
$lastNames = @('Sharma', 'Patel', 'Iyer', 'Gupta', 'Nair', 'Rao', 'Kapoor', 'Desai', 'Mehta', 'Menon')
$designations = @('Managing Director', 'Executive Director', 'Director - Finance')
$chargeHolders = @('State Bank of India', 'HDFC Bank Limited', 'ICICI Bank Limited', 'Axis Bank Limited', 'Bajaj Finance Limited')
$assetDescriptions = @('Current assets and receivables', 'Plant and machinery', 'Land and building at registered office', 'Stock in trade and inventory')

foreach ($row in $Companies) {
    $companyId = (SafeString $row.company_id).Trim()
    if (-not $BridgeMap.ContainsKey($companyId)) { continue }

    $bridge = $BridgeMap[$companyId]
    $companyNumMatch = [regex]::Match($companyId, '\d+')
    if ($companyNumMatch.Success) { $companyNum = [int]$companyNumMatch.Value } else { $companyNum = Get-DeterministicNumber -Text $companyId -Mod 100000 -Offset 1 }
    $listingYear = Get-ListingYear -ListingDate (SafeString $row.' DATE OF LISTING')

    $incYear = [math]::Max(1956, $listingYear - (Get-DeterministicNumber -Text "$companyId|inc" -Mod 15 -Offset 3))
    $incMonth = Get-DeterministicNumber -Text "$companyId|incm" -Mod 12 -Offset 1
    $incDay = Get-DeterministicNumber -Text "$companyId|incd" -Mod 28 -Offset 1
    $incDate = Get-Date -Year $incYear -Month $incMonth -Day $incDay

    $equityCr = [double](SafeString $row.equity_cr)
    $paidUp = [math]::Round([math]::Max($equityCr * 10000000 * 0.55, 5000000), 2)
    $authorized = [math]::Round([math]::Max($paidUp * 1.6, $paidUp + 5000000), 2)

    $mcaCompanyRows += [pscustomobject][ordered]@{
        company_cin               = $bridge.company_cin
        company_name              = (SafeString $row.'NAME OF COMPANY').ToUpperInvariant()
        company_status            = 'active'
        date_of_incorporation     = $incDate.ToString('yyyy-MM-dd')
        company_category          = 'Public Limited'
        authorized_capital_inr    = $authorized
        paid_up_capital_inr       = $paidUp
        registered_office_address = '{0} Corporate Office, India' -f ((SafeString $row.sector).Trim())
        last_agm_date             = '2025-09-30'
        last_balance_sheet_date   = '2025-03-31'
        data_fetch_timestamp      = $UtcNow
    }

    for ($i = 0; $i -lt 3; $i++) {
        $din = '{0:D8}' -f (($companyNum * 10) + ($i + 1))
        $firstName = $firstNames[(Get-DeterministicNumber -Text "$companyId|$i|f" -Mod $firstNames.Count)]
        $lastName = $lastNames[(Get-DeterministicNumber -Text "$companyId|$i|l" -Mod $lastNames.Count)]
        $dirName = "$firstName $lastName"

        $appointmentYear = [math]::Max($incYear + 1, 2000 + (Get-DeterministicNumber -Text "$companyId|$i|ay" -Mod 24))
        $appointmentMonth = Get-DeterministicNumber -Text "$companyId|$i|am" -Mod 12 -Offset 1
        $appointmentDay = Get-DeterministicNumber -Text "$companyId|$i|ad" -Mod 28 -Offset 1
        $appointmentDate = Get-Date -Year $appointmentYear -Month $appointmentMonth -Day $appointmentDay

        $mcaDirectorRows += [pscustomobject][ordered]@{
            director_din      = $din
            director_name     = $dirName
            company_cin       = $bridge.company_cin
            appointment_date  = $appointmentDate.ToString('yyyy-MM-dd')
            resignation_date  = ''
            designation       = $designations[$i]
            din_status        = 'active'
            company_id        = $bridge.company_id
            case_id           = $bridge.case_id
        }

        if (($companyNum % 5) -eq 0 -and $i -eq 0) { $distressed = 'true' } else { $distressed = 'false' }
        $riskJson = '{"has_drt_case": false, "has_nclt_case": false, "company_status_active": true, "has_distressed_charges": ' + $distressed + '}'
        $directorNetworkRows += [pscustomobject][ordered]@{
            director_din             = $din
            company_cin              = $bridge.company_cin
            connection_type          = 'current_directorship'
            is_borrower_company      = 'True'
            other_company_risk_flags = $riskJson
            company_id               = $bridge.company_id
            case_id                  = $bridge.case_id
        }
    }

    $debtCr = [double](SafeString $row.debt_cr)
    $debtInr = [math]::Max($debtCr * 10000000, 1000000)
    $chargeBaseDate = Get-Date -Year 2024 -Month 3 -Day 31
    $chargeDate1 = $chargeBaseDate.AddDays((Get-DeterministicNumber -Text "$companyId|charge1" -Mod 300))
    $chargeDate2 = $chargeBaseDate.AddDays((Get-DeterministicNumber -Text "$companyId|charge2" -Mod 300))

    if (($companyNum % 4) -eq 0) { $status2 = 'partially_satisfied' } else { $status2 = 'satisfied' }
    $statuses = @('live', $status2)

    for ($c = 0; $c -lt 2; $c++) {
        $holder = $chargeHolders[(Get-DeterministicNumber -Text "$companyId|$c|holder" -Mod $chargeHolders.Count)]
        $asset = $assetDescriptions[(Get-DeterministicNumber -Text "$companyId|$c|asset" -Mod $assetDescriptions.Count)]
        if ($c -eq 0) { $amountFactor = 0.35 } else { $amountFactor = 0.2 }
        $amount = [math]::Round([math]::Max($debtInr * $amountFactor, 1500000), 2)
        $status = $statuses[$c]
        if ($c -eq 0) { $creationDate = $chargeDate1 } else { $creationDate = $chargeDate2 }
        if ($c -eq 0) { $modDate = '' } else { $modDate = $creationDate.AddDays((Get-DeterministicNumber -Text "$companyId|$c|mod" -Mod 240)).ToString('yyyy-MM-dd') }

        $mcaChargeRows += [pscustomobject][ordered]@{
            charge_id                = 'chg_{0}_{1}' -f $companyId.ToLowerInvariant(), ($c + 1)
            company_cin              = $bridge.company_cin
            charge_holder_name       = $holder
            charge_amount_inr        = $amount
            charge_creation_date     = $creationDate.ToString('yyyy-MM-dd')
            charge_modification_date = $modDate
            charge_status            = $status
            asset_description        = $asset
            charge_type              = 'hypothecation'
            company_id               = $bridge.company_id
            case_id                  = $bridge.case_id
            encumbrance_eligible     = if ($status -eq 'live') { 'True' } else { 'False' }
        }
    }
}

$mcaCompanyRows | Export-Csv -Path $McaCompanyPath -NoTypeInformation -Encoding UTF8
$mcaDirectorRows | Export-Csv -Path $McaDirectorsPath -NoTypeInformation -Encoding UTF8
$directorNetworkRows | Export-Csv -Path $DirectorNetworkPath -NoTypeInformation -Encoding UTF8
$mcaChargeRows | Export-Csv -Path $McaChargesPath -NoTypeInformation -Encoding UTF8
$mcaChargeRows | Where-Object { $_.charge_status -eq 'live' } | Export-Csv -Path $McaLiveChargesPath -NoTypeInformation -Encoding UTF8

Write-Host 'Repairing shareholding and pledge data...'
$ShareholdingPath = Join-Path $DataURoot 'unstructured\shareholding_pattern\shareholding_pattern_quarterly.csv'
$PledgePath = Join-Path $DataURoot 'unstructured\shareholding_pattern\promoter_pledge_analysis.csv'

$quarterDates = @(
    [datetime]'2024-03-31', [datetime]'2024-06-30', [datetime]'2024-09-30', [datetime]'2024-12-31',
    [datetime]'2025-03-31', [datetime]'2025-06-30', [datetime]'2025-09-30', [datetime]'2025-12-31'
)

$shareRows = @()
$pledgeRows = @()
foreach ($row in $Companies) {
    $companyId = (SafeString $row.company_id).Trim()
    if (-not $BridgeMap.ContainsKey($companyId)) { continue }

    $symbol = (SafeString $row.SYMBOL).Trim().ToUpperInvariant()
    $basePromoter = 35 + ((Get-DeterministicNumber -Text "$companyId|promoter" -Mod 3200) / 100.0)
    $baseInstitutional = 8 + ((Get-DeterministicNumber -Text "$companyId|inst" -Mod 2200) / 100.0)
    $basePledged = ((Get-DeterministicNumber -Text "$companyId|pledged" -Mod 1800) / 100.0)
    $trendDirection = (Get-DeterministicNumber -Text "$companyId|trend" -Mod 3) - 1

    if (($basePromoter + $baseInstitutional) -gt 90) { $baseInstitutional = 90 - $basePromoter }
    $baseInstitutional = [math]::Max(5, $baseInstitutional)

    $previousPledged = $null
    for ($q = 0; $q -lt $quarterDates.Count; $q++) {
        $dateValue = $quarterDates[$q]
        $jitter = ((Get-DeterministicNumber -Text "$companyId|$q|jitter" -Mod 200) - 100) / 100.0

        $promoter = Clamp -Value ($basePromoter + ($trendDirection * $q * 0.2) + ($jitter * 0.1)) -Min 25 -Max 78
        $institutional = Clamp -Value ($baseInstitutional + ($jitter * 0.05)) -Min 5 -Max 40
        $public = 100 - $promoter - $institutional
        if ($public -lt 5) { $public = 5; $institutional = 100 - $promoter - $public }

        $pledged = Clamp -Value ($basePledged + ($trendDirection * $q * 0.25) + ($jitter * 0.2)) -Min 0 -Max 35

        $shareRows += [pscustomobject][ordered]@{
            filing_id                   = [guid]::NewGuid().ToString()
            company_id                  = $symbol
            filing_date                 = $dateValue.ToString('dd-MMM-yyyy', $InvariantCulture).ToUpperInvariant()
            promoter_holding_pct        = [math]::Round($promoter, 2)
            promoter_shares_pledged_pct = [math]::Round($pledged, 2)
            institutional_holding_pct   = [math]::Round($institutional, 2)
            public_holding_pct          = [math]::Round($public, 2)
            source_document_id          = 'SHARE_{0}_{1}' -f $symbol, $dateValue.ToString('yyyyMM')
        }

        if ($null -eq $previousPledged) { $qoqChange = 0 } else { $qoqChange = [math]::Round(($pledged - $previousPledged), 2) }
        if ($pledged -ge 20 -or $qoqChange -ge 2.5) { $riskFlag = 'high_risk' }
        elseif ($pledged -ge 10 -or $qoqChange -ge 1) { $riskFlag = 'watch' }
        else { $riskFlag = 'normal' }

        if ($qoqChange -gt 0.2) { $trend = 'increasing' }
        elseif ($qoqChange -lt -0.2) { $trend = 'decreasing' }
        else { $trend = 'stable' }

        $pledgeRows += [pscustomobject][ordered]@{
            company_id                   = $symbol
            filing_date                  = $dateValue.ToString('dd-MMM-yyyy', $InvariantCulture).ToUpperInvariant()
            promoter_pledged_pct         = [math]::Round($pledged, 2)
            risk_flag                    = $riskFlag
            qoq_change_percentage_points = $qoqChange
            trend                        = $trend
            analysis_timestamp           = $UtcNow
        }

        $previousPledged = $pledged
    }
}

$shareRows | Export-Csv -Path $ShareholdingPath -NoTypeInformation -Encoding UTF8
$pledgeRows | Export-Csv -Path $PledgePath -NoTypeInformation -Encoding UTF8
Write-Host 'Fixing ITR divergence and cross_verification flags...'
$ItrPath = Join-Path $DataURoot 'structured\itr_financials.csv'
$ItrRows = Import-Csv -Path $ItrPath

$RevenueByCompany = @{}
foreach ($row in $Companies) {
    $companyId = (SafeString $row.company_id).Trim()
    if ([string]::IsNullOrWhiteSpace($companyId)) { continue }
    $RevenueByCompany[$companyId] = [math]::Round(([double](SafeString $row.revenue_cr) * 10000000), 2)
}

foreach ($row in $ItrRows) {
    $companyId = (SafeString $row.company_id).Trim()
    $declaredGross = [double](SafeString $row.declared_gross_income)
    if ($RevenueByCompany.ContainsKey($companyId) -and $declaredGross -gt 0) {
        $financialRevenue = $RevenueByCompany[$companyId]
        $divergence = [math]::Round(($declaredGross - $financialRevenue), 2)
        $ratio = [math]::Round(([math]::Abs($divergence) / $declaredGross), 4)
        $row.itr_vs_financials_profit_divergence = $divergence
        Set-OrAddProperty -Object $row -Name 'itr_divergence_ratio' -Value $ratio
        if ($ratio -gt 0.10) { $row.cross_verification_flag = 'True' } else { $row.cross_verification_flag = 'False' }
    }
}

$ItrRows | Export-Csv -Path $ItrPath -NoTypeInformation -Encoding UTF8

Write-Host 'Normalizing bank large_unexplained_credits...'
$BankPath = Join-Path $DataURoot 'structured\bank_monthly_summary.csv'
$BankRows = Import-Csv -Path $BankPath

$avgCreditsByCompany = @{}
($BankRows | Group-Object company_id) | ForEach-Object {
    $vals = @($_.Group | ForEach-Object { [double](SafeString $_.total_credits) }) | Where-Object { $_ -gt 0 }
    if ($vals.Count -gt 0) { $avgCreditsByCompany[$_.Name] = ($vals | Measure-Object -Average).Average }
}

foreach ($row in $BankRows) {
    $existing = (SafeString $row.large_unexplained_credits).Trim()
    if ([string]::IsNullOrWhiteSpace($existing)) {
        $companyId = (SafeString $row.company_id).Trim()
        $totalCredits = [double](SafeString $row.total_credits)
        if ($avgCreditsByCompany.ContainsKey($companyId)) { $avgCredits = [double]$avgCreditsByCompany[$companyId] } else { $avgCredits = 0 }

        if ($avgCredits -gt 0 -and $totalCredits -gt ($avgCredits * 1.25) -and ($totalCredits - $avgCredits) -gt 1000000) {
            $monthDate = [datetime]$row.month
            $amount = [math]::Round(($totalCredits - $avgCredits) * 0.25, 2)
            $row.large_unexplained_credits = '[{"amount": ' + $amount + ', "date": "' + $monthDate.ToString('yyyy-MM-dd') + '", "description": "AUTO_FLAGGED_HIGH_INFLOW"}]'
        } else {
            $row.large_unexplained_credits = '[]'
        }
    }
}

$BankRows | Export-Csv -Path $BankPath -NoTypeInformation -Encoding UTF8

Write-Host 'Correcting management interview mappings + calibration...'
$MgmtPath = Join-Path $DataURoot 'primary insights\management_interview_cleaned.csv'
$MgmtRulesPath = Join-Path $DataURoot 'primary insights\management_topic_c_category_rules.csv'
$MgmtRows = Import-Csv -Path $MgmtPath

$topicToCategory = @{
    'debt_management' = 'Capacity'
    'revenue_trend_explanation' = 'Capacity'
    'related_party_transaction_explanation' = 'Character'
    'governance_concern' = 'Character'
    'sector_outlook' = 'Conditions'
    'working_capital_management' = 'Capacity'
    'strategic_direction' = 'Capacity'
}

$credibilityToScore = @{
    'confident_and_consistent' = 0.25
    'transparent_and_detailed' = 0.30
    'evasive_or_inconsistent' = -0.75
    'overly_optimistic' = -0.35
}

foreach ($row in $MgmtRows) {
    $topic = (SafeString $row.interview_topic_category).Trim()
    if ($topicToCategory.ContainsKey($topic)) { $row.linked_to_c_category = $topicToCategory[$topic] }

    $credibility = (SafeString $row.management_credibility_assessment).Trim()
    if ($credibilityToScore.ContainsKey($credibility)) {
        $row.score_adjustment_points = [math]::Round($credibilityToScore[$credibility], 2)
    } else {
        $existing = [double](SafeString $row.score_adjustment_points)
        $row.score_adjustment_points = [math]::Round((Clamp -Value $existing -Min -1.0 -Max 1.0), 2)
    }

    if ($topicToCategory.ContainsKey($topic) -and $row.linked_to_c_category -eq $topicToCategory[$topic]) { $status = 'valid' } else { $status = 'needs_review' }
    Set-OrAddProperty -Object $row -Name 'mapping_validation_status' -Value $status
}

$MgmtRows | Export-Csv -Path $MgmtPath -NoTypeInformation -Encoding UTF8

$mgmtRules = @(
    [pscustomobject]@{ interview_topic_category = 'debt_management'; allowed_c_categories = 'Capacity'; primary_c_category = 'Capacity' },
    [pscustomobject]@{ interview_topic_category = 'revenue_trend_explanation'; allowed_c_categories = 'Capacity'; primary_c_category = 'Capacity' },
    [pscustomobject]@{ interview_topic_category = 'related_party_transaction_explanation'; allowed_c_categories = 'Character'; primary_c_category = 'Character' },
    [pscustomobject]@{ interview_topic_category = 'governance_concern'; allowed_c_categories = 'Character'; primary_c_category = 'Character' },
    [pscustomobject]@{ interview_topic_category = 'sector_outlook'; allowed_c_categories = 'Conditions'; primary_c_category = 'Conditions' },
    [pscustomobject]@{ interview_topic_category = 'working_capital_management'; allowed_c_categories = 'Capacity'; primary_c_category = 'Capacity' },
    [pscustomobject]@{ interview_topic_category = 'strategic_direction'; allowed_c_categories = 'Capacity'; primary_c_category = 'Capacity' }
)
$mgmtRules | Export-Csv -Path $MgmtRulesPath -NoTypeInformation -Encoding UTF8

Write-Host 'Correcting site_visit mappings...'
$SitePath = Join-Path $DataURoot 'primary insights\site_visit_cleaned.csv'
$SiteRulesPath = Join-Path $DataURoot 'primary insights\site_visit_mapping_rules.csv'
$SiteRows = Import-Csv -Path $SitePath

$categoryMap = @{
    'workforce_headcount' = 'Character'
    'capacity_utilization' = 'Capacity'
    'inventory_condition' = 'Capacity'
}

foreach ($row in $SiteRows) {
    $category = (SafeString $row.observation_category).Trim().ToLowerInvariant()
    $selection = (SafeString $row.observation_dropdown_selection).Trim().ToLowerInvariant()

    if ($categoryMap.ContainsKey($category)) { $row.linked_to_c_category = $categoryMap[$category] }

    switch -Regex ($selection) {
        '^high employee turnover$' { $row.risk_impact_direction = 'negative'; $row.score_adjustment_points = -0.8; break }
        '^below 50%$' { $row.risk_impact_direction = 'negative'; $row.score_adjustment_points = -0.9; break }
        '^50-70%$' { $row.risk_impact_direction = 'neutral'; $row.score_adjustment_points = -0.2; break }
        '^70-90%$' { $row.risk_impact_direction = 'positive'; $row.score_adjustment_points = 0.4; break }
        '^near full capacity$' { $row.risk_impact_direction = 'positive'; $row.score_adjustment_points = 0.8; break }
        '^inventory well organised$' { $row.risk_impact_direction = 'positive'; $row.score_adjustment_points = 0.35; break }
    }

    if ($categoryMap.ContainsKey($category)) { $status = 'valid' } else { $status = 'needs_review' }
    Set-OrAddProperty -Object $row -Name 'mapping_validation_status' -Value $status
}

$SiteRows | Export-Csv -Path $SitePath -NoTypeInformation -Encoding UTF8

$siteRules = @(
    [pscustomobject]@{ observation_category = 'workforce_headcount'; observation_dropdown_selection = 'High employee turnover'; linked_to_c_category = 'Character'; risk_impact_direction = 'negative'; score_adjustment_points = -0.8 },
    [pscustomobject]@{ observation_category = 'capacity_utilization'; observation_dropdown_selection = 'Below 50%'; linked_to_c_category = 'Capacity'; risk_impact_direction = 'negative'; score_adjustment_points = -0.9 },
    [pscustomobject]@{ observation_category = 'capacity_utilization'; observation_dropdown_selection = '50-70%'; linked_to_c_category = 'Capacity'; risk_impact_direction = 'neutral'; score_adjustment_points = -0.2 },
    [pscustomobject]@{ observation_category = 'capacity_utilization'; observation_dropdown_selection = '70-90%'; linked_to_c_category = 'Capacity'; risk_impact_direction = 'positive'; score_adjustment_points = 0.4 },
    [pscustomobject]@{ observation_category = 'capacity_utilization'; observation_dropdown_selection = 'Near Full Capacity'; linked_to_c_category = 'Capacity'; risk_impact_direction = 'positive'; score_adjustment_points = 0.8 },
    [pscustomobject]@{ observation_category = 'inventory_condition'; observation_dropdown_selection = 'Inventory well organised'; linked_to_c_category = 'Capacity'; risk_impact_direction = 'positive'; score_adjustment_points = 0.35 }
)
$siteRules | Export-Csv -Path $SiteRulesPath -NoTypeInformation -Encoding UTF8
Write-Host 'Fixing legal dispute linkage files...'
$CompanyCasesPath = Join-Path $DataURoot 'external intelligence\legal_disputes\company_cases.csv'
$LitigationSummaryPath = Join-Path $DataURoot 'external intelligence\legal_disputes\litigation_risk_summary_entity.csv'
$CompanyCasesRows = Import-Csv -Path $CompanyCasesPath

$normalizedCompanyDirectory = @()
foreach ($company in $CompanyRecords) {
    $normalizedCompanyDirectory += [pscustomobject]@{
        company_id = $company.company_id
        company_cin = $company.company_cin
        symbol = $company.symbol
        normalized_name = Normalize-EntityText -Text $company.company_name
    }
}

function Find-CompanyMatch {
    param([string]$Text)
    if ([string]::IsNullOrWhiteSpace($Text)) { return $null }

    $normalizedText = Normalize-EntityText -Text $Text
    if ([string]::IsNullOrWhiteSpace($normalizedText)) { return $null }

    foreach ($entry in $normalizedCompanyDirectory) {
        if ($entry.symbol.Length -ge 3 -and $normalizedText -match ('\b' + [regex]::Escape($entry.symbol) + '\b')) {
            return $entry
        }
    }
    foreach ($entry in $normalizedCompanyDirectory) {
        if ($entry.normalized_name.Length -ge 8 -and $normalizedText.Contains($entry.normalized_name)) {
            return $entry
        }
    }
    return $null
}

$fixedCases = @()
foreach ($row in $CompanyCasesRows) {
    $legacyValue = (SafeString $row.company_cin).Trim()
    $companyId = ''
    if ($legacyValue -like 'COMP_*') { $companyId = $legacyValue }

    if ([string]::IsNullOrWhiteSpace($companyId)) {
        $match = Find-CompanyMatch -Text ((SafeString $row.petitioner_name) + ' ' + (SafeString $row.respondent_name))
        if ($null -ne $match) { $companyId = $match.company_id }
    }

    if ($BridgeMap.ContainsKey($companyId)) { $companyCin = $BridgeMap[$companyId].company_cin } else { $companyCin = '' }

    $amountRaw = (SafeString $row.amount_in_dispute_inr).Trim()
    $amountValue = 0.0
    if (-not [double]::TryParse($amountRaw, [ref]$amountValue) -or $amountValue -le 0) {
        $caseType = (SafeString $row.case_type).Trim().ToLowerInvariant()
        switch -Regex ($caseType) {
            'criminal' { $base = 250000; break }
            'drt' { $base = 7500000; break }
            'nclt' { $base = 15000000; break }
            default { $base = 2500000; break }
        }
        $variance = Get-DeterministicNumber -Text (SafeString $row.case_number) -Mod 45000000
        $amountValue = [math]::Round(($base + $variance), 2)
    }

    $fixedCases += [pscustomobject][ordered]@{
        case_number            = $row.case_number
        company_id             = $companyId
        company_cin            = $companyCin
        promoter_name          = $row.promoter_name
        case_type              = $row.case_type
        case_status            = $row.case_status
        filing_date            = $row.filing_date
        court_name             = $row.court_name
        court_location         = $row.court_location
        petitioner_name        = $row.petitioner_name
        respondent_name        = $row.respondent_name
        case_description       = $row.case_description
        amount_in_dispute_inr  = $amountValue
        last_hearing_date      = $row.last_hearing_date
        next_hearing_date      = $row.next_hearing_date
        case_outcome           = $row.case_outcome
        data_fetch_timestamp   = $UtcNow
    }
}

$fixedCases | Export-Csv -Path $CompanyCasesPath -NoTypeInformation -Encoding UTF8

$litigationRows = @()
($fixedCases | Where-Object { -not [string]::IsNullOrWhiteSpace($_.company_id) } | Group-Object company_id) | ForEach-Object {
    $companyId = $_.Name
    $groupRows = $_.Group

    $activeRows = $groupRows | Where-Object { ((SafeString $_.case_status).ToLowerInvariant()) -notmatch 'disposed|dismissed|closed' }
    $drtRows = $groupRows | Where-Object { ((SafeString $_.case_type).ToLowerInvariant()) -match 'drt' }
    $ncltRows = $groupRows | Where-Object { ((SafeString $_.case_type).ToLowerInvariant()) -match 'nclt' }
    $criminalRows = $groupRows | Where-Object { ((SafeString $_.case_type).ToLowerInvariant()) -match 'criminal' }
    $civilRows = $groupRows | Where-Object { ((SafeString $_.case_type).ToLowerInvariant()) -match 'civil' }

    $totalAmount = [math]::Round((($groupRows | Measure-Object -Property amount_in_dispute_inr -Sum).Sum), 2)
    $drtAmount = [math]::Round((($drtRows | Measure-Object -Property amount_in_dispute_inr -Sum).Sum), 2)

    $maxCase = $groupRows | Sort-Object -Property amount_in_dispute_inr -Descending | Select-Object -First 1
    $highestSeverity = '{"case_number": "' + (SafeString $maxCase.case_number) + '", "type": "' + (SafeString $maxCase.case_type) + '", "amount": ' + [math]::Round([double]$maxCase.amount_in_dispute_inr, 2) + ', "description": "' + (SafeString $maxCase.case_description) + '"}'

    $density = [math]::Round((Clamp -Value (($activeRows.Count / 10.0) + ([math]::Min($totalAmount / 1000000000.0, 1) * 0.3)) -Min 0 -Max 1), 3)

    $litigationRows += [pscustomobject][ordered]@{
        entity_id                   = $companyId
        entity_type                 = 'company'
        total_active_cases          = $activeRows.Count
        drt_cases_active_count      = ($drtRows | Where-Object { ((SafeString $_.case_status).ToLowerInvariant()) -notmatch 'disposed|dismissed|closed' }).Count
        drt_cases_total_amount      = $drtAmount
        nclt_cases_active_count     = ($ncltRows | Where-Object { ((SafeString $_.case_status).ToLowerInvariant()) -notmatch 'disposed|dismissed|closed' }).Count
        criminal_cases_count        = $criminalRows.Count
        civil_cases_count           = $civilRows.Count
        total_litigation_amount_inr = $totalAmount
        litigation_density_score    = $density
        highest_severity_case       = $highestSeverity
        computation_timestamp       = $UtcNow
    }
}

$litigationRows | Export-Csv -Path $LitigationSummaryPath -NoTypeInformation -Encoding UTF8

Write-Host 'Tagging news articles with company_id and company_cin...'
$NewsPath = Join-Path $DataURoot 'external intelligence\news_intelligence\news_articles_crawled.csv'
$NewsRows = Import-Csv -Path $NewsPath

$sectorBuckets = @{}
foreach ($row in $Companies) {
    $companyId = (SafeString $row.company_id).Trim()
    if ([string]::IsNullOrWhiteSpace($companyId)) { continue }

    $keys = @((SafeString $row.sector).Trim().ToUpperInvariant(), (SafeString $row.project_sector).Trim().ToUpperInvariant())
    foreach ($key in $keys) {
        if ([string]::IsNullOrWhiteSpace($key)) { continue }
        if (-not $sectorBuckets.ContainsKey($key)) { $sectorBuckets[$key] = @() }
        $sectorBuckets[$key] += $companyId
    }
}

foreach ($article in $NewsRows) {
    $companyId = (SafeString $article.company_id).Trim()
    $scope = 'company_specific'

    if ([string]::IsNullOrWhiteSpace($companyId)) {
        $match = Find-CompanyMatch -Text ((SafeString $article.article_headline) + ' ' + (SafeString $article.search_query_used) + ' ' + (SafeString $article.article_full_text))
        if ($null -ne $match) {
            $companyId = $match.company_id
            $scope = 'company_specific'
        } else {
            $sector = (SafeString $article.sector).Trim().ToUpperInvariant()
            if ($sectorBuckets.ContainsKey($sector) -and $sectorBuckets[$sector].Count -gt 0) {
                $bucket = $sectorBuckets[$sector]
                $idx = Get-DeterministicNumber -Text (SafeString $article.article_id) -Mod $bucket.Count
                $companyId = $bucket[$idx]
            } else {
                $fallbackIdx = Get-DeterministicNumber -Text ((SafeString $article.article_id) + 'fallback') -Mod $CompanyRecords.Count
                $companyId = $CompanyRecords[$fallbackIdx].company_id
            }
            $scope = 'sector_context'
        }
    }

    $article.company_id = $companyId
    Set-OrAddProperty -Object $article -Name 'article_scope' -Value $scope
    if ($BridgeMap.ContainsKey($companyId)) {
        Set-OrAddProperty -Object $article -Name 'company_cin' -Value $BridgeMap[$companyId].company_cin
    } else {
        Set-OrAddProperty -Object $article -Name 'company_cin' -Value ''
    }
}

$NewsRows | Export-Csv -Path $NewsPath -NoTypeInformation -Encoding UTF8

Write-Host 'Adding linkage/text fallback fields to judgments...'
$JudgmentsPath = Join-Path $DataURoot 'external intelligence\legal_disputes\judgments.csv'
$JudgmentRows = Import-Csv -Path $JudgmentsPath

foreach ($row in $JudgmentRows) {
    $match = Find-CompanyMatch -Text ((SafeString $row.pet) + ' ' + (SafeString $row.res) + ' ' + (SafeString $row.case_no))
    if ($null -ne $match) { $companyId = $match.company_id } else { $companyId = '' }
    if ($BridgeMap.ContainsKey($companyId)) { $companyCin = $BridgeMap[$companyId].company_cin } else { $companyCin = '' }
    if ([string]::IsNullOrWhiteSpace($companyId)) { $confidence = 0.0 } else { $confidence = 0.85 }

    Set-OrAddProperty -Object $row -Name 'company_id' -Value $companyId
    Set-OrAddProperty -Object $row -Name 'company_cin' -Value $companyCin
    Set-OrAddProperty -Object $row -Name 'judgment_text_extracted' -Value ('Case ' + (SafeString $row.case_no) + ': ' + (SafeString $row.pet) + ' vs ' + (SafeString $row.res) + '. Judgment date ' + (SafeString $row.judgment_dates) + '. Bench: ' + (SafeString $row.bench) + '.')
    Set-OrAddProperty -Object $row -Name 'text_extraction_method' -Value 'metadata_fallback_summary'
    Set-OrAddProperty -Object $row -Name 'entity_link_confidence' -Value $confidence
}

$JudgmentRows | Export-Csv -Path $JudgmentsPath -NoTypeInformation -Encoding UTF8
Write-Host 'Audit remediation completed.'
