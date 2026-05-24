ALTER TABLE judge_submissions
  ADD COLUMN IF NOT EXISTS relative_mi_score FLOAT4;
