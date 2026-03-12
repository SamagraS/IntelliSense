---
name: intelli-credit-data-validation
description: End-to-end validation workflow for Indian corporate credit appraisal datasets and CAM readiness. Use when validating case-level source completeness, schema integrity, cross-table triangulation, temporal/statistical consistency, Five Cs recomputation, override auditability, and pre-CAM/post-CAM traceability in NBFC or bank lending pipelines.
---

# Intelli Credit Data Validation

Use this skill to enforce data validity at layer boundaries and before CAM generation.

## Required Inputs

- `case_id` (string)
- `validation_stage` (`layer0` | `layer1` | `layer2` | `layer3` | `pre_cam` | `post_cam` | `full`)
- `strict_mode` (boolean; block on `HIGH` if true)

## Operating Procedure

1. Load stage-appropriate rules from `references/validation-rules.md`.
2. Validate source presence and source reliability first.
3. Validate structural rules for each relevant table.
4. Run deep business checks (triangulation, statistical, temporal, label/target consistency).
5. Classify violations using the shared severity model.
6. Apply only permitted auto-remediation.
7. Recompute Five Cs and decision band before any CAM-ready decision.
8. Enforce CAM pre-gate and post-generation traceability rules.
9. Produce structured outputs with blocking reasons and row-level annotations.

## Stage Routing

- `layer0`: source presence + document workflow + core identifier/format checks.
- `layer1`: full schema/structural checks.
- `layer2`: deep validity checks (`T-*`, `S-*`, `TM-*`) and feature validity.
- `layer3`: label/target checks (`L-*`) and override audit trail.
- `pre_cam`: pre-CAM gate only.
- `post_cam`: CAM traceability and recommendation consistency only.
- `full`: run all checks in sequence.

## Non-Negotiable Rules

- Block scoring when unresolved `CRITICAL` violations exist.
- Reject CAM facts sourced from fields with `extraction_confidence < 75`.
- Require each CAM citation to map to `source_document_id` and `page_number` (or approved computed source).
- Recompute Five Cs and composite score from stored signals to prevent stale/black-box drift.
- Treat denied documents used downstream as a hard blocker.

## Output Contract

Return:

- `validation_report` JSON (shape defined in `references/validation-rules.md`)
- Row-level `validity_annotation` flags in touched datasets
- `alert_panel_items` (human-readable UI alerts)

Set:

- `pipeline_blocked = true` when any blocking rule is hit
- `cam_ready = true` only when all pre-CAM gates pass
- `scores_valid = true` only when `L-1`, `L-2`, and `L-3` pass