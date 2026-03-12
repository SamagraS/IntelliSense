param(
  [string]$Root = 'C:\Users\Samagra\Documents\IntelliSense'
)

$ErrorActionPreference = 'Stop'
$now = Get-Date
$dataRoot = Join-Path $Root 'data_u'

function SafeNum([string]$v){
  if([string]::IsNullOrWhiteSpace($v)){ return $null }
  $x = 0.0
  if([double]::TryParse($v, [ref]$x)){ return $x }
  return $null
}

function ParseDate([string]$v){
  if([string]::IsNullOrWhiteSpace($v)){ return $null }
  $d = $null
  if([datetime]::TryParse($v, [ref]$d)){ return $d }
  return $null
}

$results = [ordered]@{
  critical = @()
  high = @()
  medium = @()
  low = @()
  info = @()
}

$expectedTables = @(
  'case_metadata','gstfilings','itrfinancials','banktransactions','bankmonthlysummary','alm_data',
  'shareholding_pattern_quarterly','promoter_pledge_analysis','borrowing_profile','portfolio_performance',
  'financial_statements_line_items','computed_financial_ratios','auditor_notes','mca_company_master',
  'mca_directors','mca_charges_registered','ecourts_cases_raw','litigation_risk_summary_entity',
  'news_articles_crawled','news_risk_signals','site_visit_observations','management_interview_notes',
  'precognitive_signals','document_classification','schema_mappings'
)

$available = @{
  'case_metadata' = (Test-Path (Join-Path $dataRoot 'structured\companies_financial_scenarios.csv'))
  'gstfilings' = (Test-Path (Join-Path $dataRoot 'structured\gst_filings.csv'))
  'itrfinancials' = (Test-Path (Join-Path $dataRoot 'structured\itr_financials.csv'))
  'banktransactions' = (Test-Path (Join-Path $dataRoot 'structured\bank_transactions.csv'))
  'bankmonthlysummary' = (Test-Path (Join-Path $dataRoot 'structured\bank_monthly_summary.csv'))
  'alm_data' = (Test-Path (Join-Path $dataRoot 'alm\alm_features.csv'))
  'shareholding_pattern_quarterly' = (Test-Path (Join-Path $dataRoot 'unstructured\shareholding_pattern\shareholding_pattern_quarterly.csv'))
  'promoter_pledge_analysis' = (Test-Path (Join-Path $dataRoot 'unstructured\shareholding_pattern\promoter_pledge_analysis.csv'))
  'borrowing_profile' = (Test-Path (Join-Path $dataRoot 'structured\borrowing_profile_synthetic.csv'))
  'portfolio_performance' = (Test-Path (Join-Path $dataRoot 'structured\portfolio_performance.csv'))
  'financial_statements_line_items' = $false
  'computed_financial_ratios' = $false
  'auditor_notes' = $false
  'mca_company_master' = (Test-Path (Join-Path $dataRoot 'external intelligence\mca\mca_company_master.csv'))
  'mca_directors' = (Test-Path (Join-Path $dataRoot 'external intelligence\mca\mca_directors.csv'))
  'mca_charges_registered' = (Test-Path (Join-Path $dataRoot 'external intelligence\mca\mca_charges_registered.csv'))
  'ecourts_cases_raw' = (Test-Path (Join-Path $dataRoot 'external intelligence\legal_disputes\company_cases.csv'))
  'litigation_risk_summary_entity' = (Test-Path (Join-Path $dataRoot 'external intelligence\legal_disputes\litigation_risk_summary_entity.csv'))
  'news_articles_crawled' = (Test-Path (Join-Path $dataRoot 'external intelligence\news_intelligence\news_articles_crawled.csv'))
  'news_risk_signals' = $false
  'site_visit_observations' = (Test-Path (Join-Path $dataRoot 'primary insights\site_visit_cleaned.csv'))
  'management_interview_notes' = (Test-Path (Join-Path $dataRoot 'primary insights\management_interview_cleaned.csv'))
  'precognitive_signals' = $false
  'document_classification' = $false
  'schema_mappings' = $false
}

foreach($t in $expectedTables){
  if(-not $available[$t]){
    if($t -in @('financial_statements_line_items','computed_financial_ratios','document_classification')){
      $results.critical += "Missing required table: $t"
    } elseif($t -in @('auditor_notes','news_risk_signals','precognitive_signals','schema_mappings')){
      $results.high += "Missing validation/signal table: $t"
    } else {
      $results.medium += "Table missing (mapped optional in current pipeline): $t"
    }
  }
}

$companies = Import-Csv (Join-Path $dataRoot 'structured\companies_financial_scenarios.csv')
$bridge = Import-Csv (Join-Path $dataRoot 'structured\entity_master_bridge.csv')
$gst = Import-Csv (Join-Path $dataRoot 'structured\gst_filings.csv')
$bankSummary = Import-Csv (Join-Path $dataRoot 'structured\bank_monthly_summary.csv')
$itr = Import-Csv (Join-Path $dataRoot 'structured\itr_financials.csv')
$borrowing = Import-Csv (Join-Path $dataRoot 'structured\borrowing_profile_synthetic.csv')
$portfolio = Import-Csv (Join-Path $dataRoot 'structured\portfolio_performance.csv')
$share = Import-Csv (Join-Path $dataRoot 'unstructured\shareholding_pattern\shareholding_pattern_quarterly.csv')
$pledge = Import-Csv (Join-Path $dataRoot 'unstructured\shareholding_pattern\promoter_pledge_analysis.csv')
$site = Import-Csv (Join-Path $dataRoot 'primary insights\site_visit_cleaned.csv')
$mgmt = Import-Csv (Join-Path $dataRoot 'primary insights\management_interview_cleaned.csv')
$mcaCompany = Import-Csv (Join-Path $dataRoot 'external intelligence\mca\mca_company_master.csv')
$mcaDirectors = Import-Csv (Join-Path $dataRoot 'external intelligence\mca\mca_directors.csv')
$mcaCharges = Import-Csv (Join-Path $dataRoot 'external intelligence\mca\mca_charges_registered.csv')
$cases = Import-Csv (Join-Path $dataRoot 'external intelligence\legal_disputes\company_cases.csv')
$lit = Import-Csv (Join-Path $dataRoot 'external intelligence\legal_disputes\litigation_risk_summary_entity.csv')
$news = Import-Csv (Join-Path $dataRoot 'external intelligence\news_intelligence\news_articles_crawled.csv')
$alm = Import-Csv (Join-Path $dataRoot 'alm\alm_features.csv')

$results.info += "cases_in_master=$($companies.Count)"

$cinRegex = '^[A-Z][0-9]{5}[A-Z]{2}[0-9]{4}[A-Z]{3}[0-9]{6}$'
$panRegex = '^[A-Z]{5}[0-9]{4}[A-Z]$'
$gstRegex = '^[0-9A-Z]{15}$'

$badPan = ($companies | Where-Object { $_.pan -notmatch $panRegex }).Count
$badGst = ($companies | Where-Object { $_.gstin -notmatch $gstRegex }).Count
$badCin = ($companies | Where-Object { $_.company_cin -notmatch $cinRegex }).Count
if($badPan -gt 0){ $results.high += "Invalid PAN format rows in companies_financial_scenarios: $badPan" }
if($badGst -gt 0){ $results.high += "Invalid GSTIN format rows in companies_financial_scenarios: $badGst" }
if($badCin -gt 0){ $results.high += "Invalid CIN format rows in companies_financial_scenarios: $badCin" }

# Bridge integrity
$bridgeDup = ($bridge | Group-Object company_id | Where-Object { $_.Count -gt 1 }).Count
if($bridgeDup -gt 0){ $results.high += "entity_master_bridge duplicate company_id groups: $bridgeDup" }
if($bridge.Count -ne $companies.Count){ $results.high += "entity_master_bridge row count ($($bridge.Count)) != company master count ($($companies.Count))" }

$caseIds = $companies.case_id
$companyIds = $companies.company_id
$symbolToCompany = @{}
foreach($r in $companies){ $sym=[string]$r.SYMBOL; if(-not [string]::IsNullOrWhiteSpace($sym)){ $symbolToCompany[$sym.ToUpper()] = $r.company_id } }

# Coverage checks (adaptive)
$gstMonthByCase = @{}
foreach($r in $gst){
  $cid=[string]$r.case_id
  if(-not $gstMonthByCase.ContainsKey($cid)){ $gstMonthByCase[$cid] = New-Object 'System.Collections.Generic.HashSet[string]' }
  $d=ParseDate([string]$r.filing_month)
  if($null -ne $d){ [void]$gstMonthByCase[$cid].Add($d.ToString('yyyy-MM')) }
}
$casesGstLt12 = 0
foreach($cid in $caseIds){
  if(-not $gstMonthByCase.ContainsKey($cid) -or $gstMonthByCase[$cid].Count -lt 12){ $casesGstLt12++ }
}
if($casesGstLt12 -gt 0){ $results.critical += "Cases with <12 months GST filings: $casesGstLt12" }

$bankMonthByCase = @{}
foreach($r in $bankSummary){
  $cid=[string]$r.case_id
  if(-not $bankMonthByCase.ContainsKey($cid)){ $bankMonthByCase[$cid] = New-Object 'System.Collections.Generic.HashSet[string]' }
  $d=ParseDate([string]$r.month)
  if($null -ne $d){ [void]$bankMonthByCase[$cid].Add($d.ToString('yyyy-MM')) }
}
$casesBankLt12 = 0
foreach($cid in $caseIds){
  if(-not $bankMonthByCase.ContainsKey($cid) -or $bankMonthByCase[$cid].Count -lt 12){ $casesBankLt12++ }
}
if($casesBankLt12 -gt 0){ $results.critical += "Cases with <12 months bank summary coverage: $casesBankLt12" }

$itrYearsByCase = @{}
foreach($r in $itr){
  $cid=[string]$r.case_id
  if(-not $itrYearsByCase.ContainsKey($cid)){ $itrYearsByCase[$cid] = New-Object 'System.Collections.Generic.HashSet[string]' }
  [void]$itrYearsByCase[$cid].Add([string]$r.assessment_year)
}
$casesItrLt3 = 0
foreach($cid in $caseIds){
  if(-not $itrYearsByCase.ContainsKey($cid) -or $itrYearsByCase[$cid].Count -lt 3){ $casesItrLt3++ }
}
if($casesItrLt3 -gt 0){ $results.high += "Cases with <3 assessment years in itr_financials: $casesItrLt3" }

$mcaByCin = @{}
foreach($r in $mcaCompany){ $mcaByCin[[string]$r.company_cin] = $true }
$missingMca = ($companies | Where-Object { -not $mcaByCin.ContainsKey([string]$_.company_cin) }).Count
if($missingMca -gt 0){ $results.critical += "Cases missing mca_company_master entry by CIN: $missingMca" }

# Shareholding coverage via symbol mapping
$shareSymbols = $share | Select-Object -ExpandProperty company_id -Unique
$shareSymbolSet = @{}
foreach($s in $shareSymbols){ $shareSymbolSet[[string]$s] = $true }
$missingShare = ($companies | Where-Object { -not $shareSymbolSet.ContainsKey(([string]$_.SYMBOL).ToUpper()) }).Count
if($missingShare -gt 0){ $results.medium += "Cases missing shareholding rows by symbol mapping: $missingShare" }

# Litigation summary company + promoter coverage
$litCompanySet = @{}
$litPromoterSet = @{}
foreach($r in $lit){
  $eid=[string]$r.entity_id
  $etype=[string]$r.entity_type
  if($etype -eq 'company'){ $litCompanySet[$eid] = $true }
  if($etype -eq 'promoter'){ $litPromoterSet[$eid] = $true }
}
$missingLitCompany = ($companies | Where-Object { -not $litCompanySet.ContainsKey([string]$_.company_id) }).Count
if($missingLitCompany -gt 0){ $results.high += "Cases missing company litigation summary row: $missingLitCompany" }
if($litPromoterSet.Count -eq 0){ $results.high += "No promoter-level litigation_risk_summary_entity rows present" }

# Site and management coverage
$siteCaseSet = @{}
foreach($r in $site){ $siteCaseSet[[string]$r.case_id]=$true }
$mgmtCaseSet = @{}
foreach($r in $mgmt){ $mgmtCaseSet[[string]$r.case_id]=$true }
$missingSite = ($companies | Where-Object { -not $siteCaseSet.ContainsKey([string]$_.case_id) }).Count
$missingMgmt = ($companies | Where-Object { -not $mgmtCaseSet.ContainsKey([string]$_.case_id) }).Count
if($missingSite -gt 0){ $results.medium += "Cases missing site visit records: $missingSite" }
if($missingMgmt -gt 0){ $results.medium += "Cases missing management interview records: $missingMgmt" }

# News coverage >=10 articles per case (mapped by company_id)
$companyToCase = @{}
foreach($r in $companies){ $companyToCase[[string]$r.company_id] = [string]$r.case_id }
$newsCaseCount = @{}
foreach($r in $news){
  $comp=[string]$r.company_id
  if($companyToCase.ContainsKey($comp)){
    $cid=$companyToCase[$comp]
    if(-not $newsCaseCount.ContainsKey($cid)){ $newsCaseCount[$cid]=0 }
    $newsCaseCount[$cid]++
  }
}
$casesNewsLt10 = 0
foreach($cid in $caseIds){
  if(-not $newsCaseCount.ContainsKey($cid) -or $newsCaseCount[$cid] -lt 10){ $casesNewsLt10++ }
}
if($casesNewsLt10 -gt 0){ $results.medium += "Cases with <10 news articles: $casesNewsLt10" }

# GST structural checks
$gstDup = ($gst | Group-Object case_id,gstin,filing_month | Where-Object { $_.Count -gt 1 }).Count
if($gstDup -gt 0){ $results.high += "gst_filings duplicate (case_id,gstin,filing_month) groups: $gstDup" }
$gstBadStatus = ($gst | Where-Object { $_.filing_status -notin @('filed','missing','late_filed') }).Count
$gstBadDiv = ($gst | Where-Object { $v=SafeNum([string]$_.gstr2a_vs_3b_divergence_pct); $null -eq $v -or $v -lt -200 -or $v -gt 200 }).Count
$gstNeg = ($gst | Where-Object {
  ($a=SafeNum([string]$_.gstr3b_revenue_declared); $null -eq $a -or $a -lt 0) -or
  ($b=SafeNum([string]$_.gstr2a_reported_purchases); $null -eq $b -or $b -lt 0) -or
  ($c=SafeNum([string]$_.gstr3b_tax_paid); $null -eq $c -or $c -lt 0)
}).Count
if($gstBadStatus -gt 0){ $results.high += "gst_filings invalid filing_status rows: $gstBadStatus" }
if($gstBadDiv -gt 0){ $results.high += "gst_filings divergence out of bounds rows: $gstBadDiv" }
if($gstNeg -gt 0){ $results.high += "gst_filings negative/null monetary rows: $gstNeg" }

# ITR checks
$itrDup = ($itr | Group-Object case_id,assessment_year | Where-Object { $_.Count -gt 1 }).Count
if($itrDup -gt 0){ $results.high += "itr_financials duplicate (case_id,assessment_year) groups: $itrDup" }
$itrGrossNet = ($itr | Where-Object {
  $g=SafeNum([string]$_.declared_gross_income); $n=SafeNum([string]$_.declared_net_income)
  ($null -eq $g) -or ($null -eq $n) -or ($g -lt $n)
}).Count
if($itrGrossNet -gt 0){ $results.high += "itr_financials gross<net or null rows: $itrGrossNet" }
$itrAyFyMismatch = 0
foreach($r in $itr){
  $ay=[string]$r.assessment_year
  $fy=[string]$r.financial_year
  if($ay -match '^AY(\d{4})-\d{2}$'){
    $start=[int]$Matches[1]-1
    $end2=($start+1).ToString().Substring(2,2)
    $expected = "FY$start-$end2"
    if($fy -ne $expected){ $itrAyFyMismatch++ }
  }
}
if($itrAyFyMismatch -gt 0){ $results.medium += "itr_financials AY/FY consistency mismatches: $itrAyFyMismatch" }

# Borrowing profile checks (adaptive field names)
$borrowDup = ($borrowing | Group-Object case_id,lender_name,facility_category | Where-Object { $_.Count -gt 1 }).Count
if($borrowDup -gt 0){ $results.medium += "borrowing_profile duplicate lender+facility groups: $borrowDup" }
$borrowBadAmt = ($borrowing | Where-Object {
  $s=SafeNum([string]$_.sanctioned_amount); $o=SafeNum([string]$_.outstanding_amount)
  ($null -eq $s) -or ($s -le 0) -or ($null -eq $o) -or ($o -lt 0) -or ($o -gt $s)
}).Count
if($borrowBadAmt -gt 0){ $results.high += "borrowing_profile amount consistency violations: $borrowBadAmt" }
$borrowBadIr = ($borrowing | Where-Object { $v=SafeNum([string]$_.interest_rate_pct); $null -eq $v -or $v -lt 0 -or $v -gt 50 }).Count
if($borrowBadIr -gt 0){ $results.high += "borrowing_profile invalid interest_rate_pct rows: $borrowBadIr" }
$borrowPaymentBad = ($borrowing | Where-Object { $_.payment_track_record -notin @('regular','irregular','arrears','default') }).Count
if($borrowPaymentBad -gt 0){ $results.medium += "borrowing_profile unexpected payment_track_record rows: $borrowPaymentBad" }

# Portfolio checks + NBFC coverage
$nbfcCases = $companies | Where-Object { ([string]$_.project_sector).ToUpper() -eq 'NBFC' }
$nbfcCaseSet = @{}
foreach($r in $nbfcCases){ $nbfcCaseSet[[string]$r.case_id]=$true }
$portfolioCaseSet = @{}
foreach($r in $portfolio){ $portfolioCaseSet[[string]$r.case_id]=$true }
$nbfcMissingPortfolio = 0
foreach($cid in $nbfcCaseSet.Keys){ if(-not $portfolioCaseSet.ContainsKey($cid)){ $nbfcMissingPortfolio++ } }
if($nbfcMissingPortfolio -gt 0){ $results.high += "NBFC cases missing portfolio_performance: $nbfcMissingPortfolio" }
$portfolioBad = ($portfolio | Where-Object {
  ($g=SafeNum([string]$_.gross_npa_pct); $null -eq $g -or $g -lt 0 -or $g -gt 100) -or
  ($n=SafeNum([string]$_.net_npa_pct); $null -eq $n -or $n -lt 0 -or $n -gt 100 -or $n -gt $g) -or
  ($p=SafeNum([string]$_.provisioning_coverage_ratio); $null -eq $p -or $p -lt 0 -or $p -gt 100)
}).Count
if($portfolioBad -gt 0){ $results.high += "portfolio_performance range consistency violations: $portfolioBad" }

# ALM checks (wide-format adaptation)
$almCaseSet = @{}
foreach($r in $alm){ $almCaseSet[[string]$r.case_id]=$true }
$nbfcMissingAlm = 0
foreach($cid in $nbfcCaseSet.Keys){ if(-not $almCaseSet.ContainsKey($cid)){ $nbfcMissingAlm++ } }
if($nbfcMissingAlm -gt 0){ $results.high += "NBFC cases missing ALM features: $nbfcMissingAlm" }

$almNegBuckets = 0
$bucketCols = @('assets_bucket_inr_1-7d','assets_bucket_inr_8-14d','assets_bucket_inr_15-30d','assets_bucket_inr_1-3m','assets_bucket_inr_3-6m','assets_bucket_inr_6-12m','assets_bucket_inr_1-3y','assets_bucket_inr_>3y','liabilities_bucket_inr_1-7d','liabilities_bucket_inr_8-14d','liabilities_bucket_inr_15-30d','liabilities_bucket_inr_1-3m','liabilities_bucket_inr_3-6m','liabilities_bucket_inr_6-12m','liabilities_bucket_inr_1-3y','liabilities_bucket_inr_>3y')
foreach($r in $alm){
  foreach($c in $bucketCols){
    $v = SafeNum([string]$r.$c)
    if($null -eq $v -or $v -lt 0){ $almNegBuckets++; break }
  }
}
if($almNegBuckets -gt 0){ $results.high += "alm_features rows with null/negative bucket values: $almNegBuckets" }

# Shareholding checks
$shareBadRange = ($share | Where-Object {
  ($a=SafeNum([string]$_.promoter_holding_pct); $null -eq $a -or $a -lt 0 -or $a -gt 100) -or
  ($b=SafeNum([string]$_.institutional_holding_pct); $null -eq $b -or $b -lt 0 -or $b -gt 100) -or
  ($c=SafeNum([string]$_.public_holding_pct); $null -eq $c -or $c -lt 0 -or $c -gt 100)
}).Count
if($shareBadRange -gt 0){ $results.high += "shareholding rows with pct out of [0,100]: $shareBadRange" }
$shareBadSum = ($share | Where-Object {
  $a=SafeNum([string]$_.promoter_holding_pct); $b=SafeNum([string]$_.institutional_holding_pct); $c=SafeNum([string]$_.public_holding_pct)
  ($a + $b + $c) -lt 99 -or ($a + $b + $c) -gt 101
}).Count
if($shareBadSum -gt 0){ $results.medium += "shareholding rows where promoter+institutional+public not ~100: $shareBadSum" }
$sharePledgeGtProm = ($share | Where-Object {
  (SafeNum([string]$_.promoter_shares_pledged_pct)) -gt (SafeNum([string]$_.promoter_holding_pct))
}).Count
if($sharePledgeGtProm -gt 0){ $results.high += "shareholding rows with pledged pct > promoter holding pct: $sharePledgeGtProm" }

# Pledge analysis checks (adaptive risk_flag mapping)
$allowedRisk = @('normal','caution','high_risk','critical','watch')
$pledgeBadRisk = ($pledge | Where-Object { ([string]$_.risk_flag).ToLower() -notin $allowedRisk }).Count
if($pledgeBadRisk -gt 0){ $results.medium += "promoter_pledge_analysis invalid risk_flag values: $pledgeBadRisk" }

$pledgeTrendMismatch = 0
$pledgeBySymbol = $pledge | Group-Object company_id
foreach($g in $pledgeBySymbol){
  $rows = $g.Group | Sort-Object { ParseDate([string]$_.filing_date) }
  $prev = $null
  foreach($r in $rows){
    $curr = SafeNum([string]$r.promoter_pledged_pct)
    $qoq = SafeNum([string]$r.qoq_change_percentage_points)
    $trend = ([string]$r.trend).ToLower()
    if($null -eq $curr -or $null -eq $qoq){ continue }

    if($null -ne $prev){
      $calc = [math]::Round(($curr - $prev),2)
      if([math]::Abs($calc - $qoq) -gt 0.51){ $pledgeTrendMismatch++ }
    }

    if(($qoq -gt 0.5 -and $trend -ne 'increasing') -or ($qoq -lt -0.5 -and $trend -ne 'decreasing') -or ([math]::Abs($qoq) -le 0.5 -and $trend -ne 'stable')){
      $pledgeTrendMismatch++
    }

    $prev = $curr
  }
}
if($pledgeTrendMismatch -gt 0){ $results.medium += "promoter_pledge_analysis qoq/trend consistency mismatches: $pledgeTrendMismatch" }

# MCA checks
$mcaBadStatus = ($mcaCompany | Where-Object { ([string]$_.company_status).ToLower() -notin @('active','strike_off','amalgamated','dissolved','under_liquidation') }).Count
if($mcaBadStatus -gt 0){ $results.high += "mca_company_master invalid company_status rows: $mcaBadStatus" }
$mcaNonActive = ($mcaCompany | Where-Object { ([string]$_.company_status).ToLower() -ne 'active' }).Count
if($mcaNonActive -gt 0){ $results.critical += "mca_company_master non-active companies present: $mcaNonActive" }

$directorDup = ($mcaDirectors | Group-Object director_din,company_cin | Where-Object { $_.Count -gt 1 }).Count
if($directorDup -gt 0){ $results.medium += "mca_directors duplicate (director_din,company_cin) groups: $directorDup" }
$disqualifiedDin = ($mcaDirectors | Where-Object { ([string]$_.din_status).ToLower() -eq 'disqualified' }).Count
if($disqualifiedDin -gt 0){ $results.critical += "mca_directors disqualified DIN rows: $disqualifiedDin" }

$chargesBadStatus = ($mcaCharges | Where-Object { ([string]$_.charge_status).ToLower() -notin @('live','satisfied','partially_satisfied') }).Count
if($chargesBadStatus -gt 0){ $results.high += "mca_charges_registered invalid charge_status rows: $chargesBadStatus" }
$chargesBadType = ($mcaCharges | Where-Object { ([string]$_.charge_type).ToLower() -notin @('mortgage','hypothecation','pledge','other') }).Count
if($chargesBadType -gt 0){ $results.high += "mca_charges_registered invalid charge_type rows: $chargesBadType" }

# eCourts substitute checks on company_cases
$caseDup = ($cases | Group-Object case_number | Where-Object { $_.Count -gt 1 }).Count
if($caseDup -gt 0){ $results.high += "company_cases duplicate case_number groups: $caseDup" }
$caseFuture = ($cases | Where-Object { $d=ParseDate([string]$_.filing_date); $null -ne $d -and $d -gt $now }).Count
if($caseFuture -gt 0){ $results.high += "company_cases filing_date in future rows: $caseFuture" }
$caseTypeBad = ($cases | Where-Object { ([string]$_.case_type).ToLower() -notin @('drt','nclt','civil','criminal','arbitration','consumer_dispute','other') }).Count
if($caseTypeBad -gt 0){ $results.medium += "company_cases case_type outside expected enum rows: $caseTypeBad" }

# Litigation summary recompute check (company only)
$activeCounts = @{}
$drtActiveCounts = @{}
foreach($r in $cases){
  $cid=[string]$r.company_id
  $status=([string]$r.case_status).ToLower()
  $type=([string]$r.case_type).ToLower()
  if($status -eq 'active'){
    if(-not $activeCounts.ContainsKey($cid)){ $activeCounts[$cid]=0 }
    $activeCounts[$cid]++
    if($type -eq 'drt'){
      if(-not $drtActiveCounts.ContainsKey($cid)){ $drtActiveCounts[$cid]=0 }
      $drtActiveCounts[$cid]++
    }
  }
}
$litMismatch = 0
foreach($r in $lit | Where-Object { $_.entity_type -eq 'company' }){
  $cid=[string]$r.entity_id
  $stored = [int](SafeNum([string]$r.total_active_cases))
  $calc = 0
  if($activeCounts.ContainsKey($cid)){ $calc=$activeCounts[$cid] }
  if($stored -ne $calc){ $litMismatch++ }
}
if($litMismatch -gt 0){ $results.high += "litigation_risk_summary_entity total_active_cases mismatches vs company_cases: $litMismatch" }

# News checks
$newsBadPhase = ($news | Where-Object { ([string]$_.crawl_phase) -notin @('background_deep_crawl','live_refresh') }).Count
if($newsBadPhase -gt 0){ $results.medium += "news_articles_crawled invalid crawl_phase rows: $newsBadPhase" }
$newsFuture = ($news | Where-Object { $d=ParseDate([string]$_.published_date); $null -ne $d -and $d -gt $now }).Count
if($newsFuture -gt 0){ $results.high += "news_articles_crawled future published_date rows: $newsFuture" }
$newsEmptyText = ($news | Where-Object { [string]::IsNullOrWhiteSpace([string]$_.article_full_text) }).Count
if($newsEmptyText -gt 0){ $results.high += "news_articles_crawled empty article_full_text rows: $newsEmptyText" }

# Site + management checks
$siteBadDir = ($site | Where-Object {
  $dir=([string]$_.risk_impact_direction).ToLower(); $s=SafeNum([string]$_.score_adjustment_points)
  ($null -eq $s) -or
  ($dir -eq 'positive' -and $s -le 0) -or
  ($dir -eq 'negative' -and $s -ge 0) -or
  ($dir -eq 'neutral' -and [math]::Abs($s) -gt 0.001)
}).Count
if($siteBadDir -gt 0){ $results.medium += "site_visit direction vs score mismatch rows: $siteBadDir" }
$siteOldPending = ($site | Where-Object {
  $d=ParseDate([string]$_.visit_date)
  $d -ne $null -and (([string]$_.verification_status).ToLower() -eq 'pending') -and (($now - $d).TotalDays -gt 7)
}).Count
if($siteOldPending -gt 0){ $results.low += "site_visit pending verification older than 7 days rows: $siteOldPending" }

$mgmtBadScore = ($mgmt | Where-Object {
  $ass=([string]$_.management_credibility_assessment).ToLower(); $s=SafeNum([string]$_.score_adjustment_points)
  ($null -eq $s) -or
  ($ass -eq 'confident_and_consistent' -and $s -lt 0) -or
  ($ass -eq 'evasive_or_inconsistent' -and ($s -gt -0.5 -or $s -lt -1.5))
}).Count
if($mgmtBadScore -gt 0){ $results.medium += "management_interview credibility vs score mismatch rows: $mgmtBadScore" }
$mgmtOldPending = ($mgmt | Where-Object {
  $d=ParseDate([string]$_.interview_date)
  $req=([string]$_.requires_document_verification).ToLower()
  $stat=([string]$_.verification_status).ToLower()
  $d -ne $null -and $req -eq 'true' -and $stat -eq 'pending' -and (($now - $d).TotalDays -gt 3)
}).Count
if($mgmtOldPending -gt 0){ $results.low += "management_interview pending verification >3 days rows: $mgmtOldPending" }

# Stream validation for bank_transactions + consistency with monthly summary
$bankTxPath = Join-Path $dataRoot 'structured\bank_transactions.csv'
$parser = New-Object Microsoft.VisualBasic.FileIO.TextFieldParser($bankTxPath)
$parser.TextFieldType = [Microsoft.VisualBasic.FileIO.FieldType]::Delimited
$parser.SetDelimiters(',')
$parser.HasFieldsEnclosedInQuotes = $true
$header = $parser.ReadFields()
$idx = @{}
for($i=0;$i -lt $header.Length;$i++){ $idx[$header[$i]] = $i }

$badType = 0; $badAmount = 0; $futureTx = 0; $nullCore = 0; $odOver1Cr = 0; $idPatternMismatch = 0
$rowNum = 0
$creditAgg = @{}
$debitAgg = @{}

while(-not $parser.EndOfData){
  $f = $parser.ReadFields()
  $rowNum++
  $txid = [string]$f[$idx['transaction_id']]
  if($txid -ne "TXN_$rowNum"){ $idPatternMismatch++ }

  $case = [string]$f[$idx['case_id']]
  $comp = [string]$f[$idx['company_id']]
  $tdate = ParseDate([string]$f[$idx['transaction_date']])
  $ttype = ([string]$f[$idx['transaction_type']]).ToLower()
  $amt = SafeNum([string]$f[$idx['amount']])
  $acct = [string]$f[$idx['bank_account_no']]
  $rb = SafeNum([string]$f[$idx['running_balance']])

  if([string]::IsNullOrWhiteSpace($case) -or [string]::IsNullOrWhiteSpace($comp) -or [string]::IsNullOrWhiteSpace($acct) -or $null -eq $tdate -or $null -eq $amt){ $nullCore++ }
  if($ttype -notin @('credit','debit')){ $badType++ }
  if($null -eq $amt -or $amt -le 0){ $badAmount++ }
  if($null -ne $tdate -and $tdate -gt $now){ $futureTx++ }
  if($null -ne $rb -and $rb -lt -10000000){ $odOver1Cr++ }

  if($null -ne $tdate -and $null -ne $amt -and $ttype -in @('credit','debit')){
    $key = "$case|$comp|$($tdate.ToString('yyyy-MM'))"
    if($ttype -eq 'credit'){
      if(-not $creditAgg.ContainsKey($key)){ $creditAgg[$key] = 0.0 }
      $creditAgg[$key] += $amt
    } else {
      if(-not $debitAgg.ContainsKey($key)){ $debitAgg[$key] = 0.0 }
      $debitAgg[$key] += $amt
    }
  }
}
$parser.Close()

if($badType -gt 0){ $results.high += "bank_transactions invalid transaction_type rows: $badType" }
if($badAmount -gt 0){ $results.high += "bank_transactions non-positive amount rows: $badAmount" }
if($futureTx -gt 0){ $results.high += "bank_transactions future transaction_date rows: $futureTx" }
if($nullCore -gt 0){ $results.high += "bank_transactions null core field rows: $nullCore" }
if($odOver1Cr -gt 0){ $results.medium += "bank_transactions rows with running_balance < -1Cr: $odOver1Cr" }
if($idPatternMismatch -gt 0){ $results.medium += "bank_transactions transaction_id sequence pattern mismatches: $idPatternMismatch" }

$summaryMismatch = 0
$summaryMissingInTx = 0
foreach($r in $bankSummary){
  $d = ParseDate([string]$r.month)
  if($null -eq $d){ continue }
  $key = "{0}|{1}|{2}" -f [string]$r.case_id,[string]$r.company_id,$d.ToString('yyyy-MM')

  $sumC = 0.0; if($creditAgg.ContainsKey($key)){ $sumC = [double]$creditAgg[$key] } else { $summaryMissingInTx++ }
  $sumD = 0.0; if($debitAgg.ContainsKey($key)){ $sumD = [double]$debitAgg[$key] }

  $rowC = SafeNum([string]$r.total_credits); if($null -eq $rowC){ $rowC = 0 }
  $rowD = SafeNum([string]$r.total_debits); if($null -eq $rowD){ $rowD = 0 }

  $tolC = [math]::Max(1.0, [math]::Abs($sumC) * 0.005)
  $tolD = [math]::Max(1.0, [math]::Abs($sumD) * 0.005)
  if([math]::Abs($rowC - $sumC) -gt $tolC -or [math]::Abs($rowD - $sumD) -gt $tolD){ $summaryMismatch++ }
}
if($summaryMismatch -gt 0){ $results.high += "bank_monthly_summary vs bank_transactions aggregate mismatches (>0.5%): $summaryMismatch" }
if($summaryMissingInTx -gt 0){ $results.medium += "bank_monthly_summary rows with no matching tx aggregate key: $summaryMissingInTx" }

# Final output compact JSON
$output = [ordered]@{
  timestamp = (Get-Date).ToString('s')
  critical_count = $results.critical.Count
  high_count = $results.high.Count
  medium_count = $results.medium.Count
  low_count = $results.low.Count
  critical = $results.critical
  high = $results.high
  medium = $results.medium
  low = $results.low
  info = $results.info
}

$output | ConvertTo-Json -Depth 6
