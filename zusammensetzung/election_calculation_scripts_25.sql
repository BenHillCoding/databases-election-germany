-- German 2025 Federal Election Seat Allocation using Views
-- Based on Sainte-Laguë/Schepers method (630 seats, no leveling seats)

-- VIEW 1: Aggregate second votes (Zweitstimmen) per party nationally
CREATE OR REPLACE VIEW v_national_votes AS
SELECT
  p.id as partei_id,
  p.name as partei_name,
  count(z.id) as total_votes,
  ROUND((count(z.id)::DECIMAL / (SELECT count(zweitstimme.id) FROM zweitstimme JOIN wahlkreisergebnis ON zweitstimme.wahlkreisergebnis_id = wahlkreisergebnis.id WHERE gueltig = true AND wahlkreisergebnis.wahl_id = 21))::NUMERIC * 100, 2) as vote_share_percent
FROM zweitstimme z
JOIN partei p ON z.partei_id = p.id
JOIN wahlkreisergebnis we ON we.id = z.wahlkreisergebnis_id
WHERE z.gueltig = true AND we.wahl_id = 21
GROUP BY p.id, p.name;

-- VIEW 2: Count direct mandates per party nationally
CREATE OR REPLACE VIEW v_national_direct_mandates AS
SELECT
  p.id as partei_id,
  p.name as partei_name,
  COUNT(*) as direct_mandate_count
FROM direktkandidatur dk
JOIN partei p ON dk.partei_id = p.id
WHERE dk.wahl_id = 21
GROUP BY p.id, p.name;

-- View 2.5: Calculate how many wahlkreise each party won erstimmen wise
CREATE OR REPLACE VIEW v_partei_wahlkreisgewinne AS
WITH stimmen_pro_kandidat AS (
    SELECT
        e.wahlkreisergebnis_id,
        dk.id AS direktkandidatur_id,
        dk.partei_id,
        wk.id AS wahlkreis_id,
        count(e.id) AS stimmen
    FROM erststimme e
    JOIN direktkandidatur dk ON e.direktkandidatur_id = dk.id
    JOIN wahlkreis wk ON dk.wahlkreis_id = wk.id
	  JOIN wahlkreisergebnis wke ON wke.id = e.wahlkreisergebnis_id
    WHERE e.gueltig = true AND wke.wahl_id = 21
    GROUP BY e.wahlkreisergebnis_id, dk.id, dk.partei_id, wk.id
),
rangliste AS (
    SELECT
        spk.*,
        RANK() OVER (PARTITION BY spk.wahlkreis_id ORDER BY spk.stimmen DESC) AS rang
    FROM stimmen_pro_kandidat spk
)
SELECT
    partei_id,
    COUNT(*) AS gewonnene_wahlkreise
FROM rangliste
WHERE rang = 1
GROUP BY partei_id
ORDER BY gewonnene_wahlkreise DESC;

-- VIEW 3: Determine qualifying parties
-- Parties qualify if: vote_share >= 5% OR are recognized minority parties (e.g., SSW)
CREATE OR REPLACE VIEW v_qualifying_parties AS
SELECT
  nv.partei_id,
  nv.partei_name,
  nv.total_votes,
  nv.vote_share_percent,
  COALESCE(ndm.direct_mandate_count, 0) as direct_mandate_count,
  p.nationale_minderheit
FROM v_national_votes nv
LEFT JOIN v_national_direct_mandates ndm ON nv.partei_id = ndm.partei_id
JOIN partei p ON nv.partei_id = p.id
LEFT JOIN v_partei_wahlkreisgewinne wkg ON p.id = wkg.partei_id
WHERE nv.vote_share_percent >= 5.0
   OR p.nationale_minderheit = true
   OR COALESCE(wkg.gewonnene_wahlkreise, 0) >= 3;

-- VIEW 4: Aggregate second votes per party and Bundesland
CREATE OR REPLACE VIEW v_party_bundesland_votes AS
SELECT
  p.id as partei_id,
  p.name as partei_name,
  b.id as bundesland_id,
  b.name as bundesland_name,
  COUNT(z.id) as second_votes
FROM zweitstimme z
JOIN partei p ON z.partei_id = p.id
JOIN wahlkreisergebnis wr ON z.wahlkreisergebnis_id = wr.id
JOIN wahlkreis wk ON wr.wahlkreis_id = wk.nummer
JOIN bundesland b ON wk.bundesland_id = b.id
WHERE z.gueltig = true
  AND p.id IN (SELECT partei_id FROM v_qualifying_parties)
  AND wr.wahl_id = 21
GROUP BY p.id, p.name, b.id, b.name;

-- VIEW 5: Calculate direct mandates per party/Bundesland
CREATE OR REPLACE VIEW v_direct_mandates AS
SELECT
  p.id as partei_id,
  p.name as partei_name,
  b.id as bundesland_id,
  b.name as bundesland_name,
  COUNT(*) as direct_mandate_count
FROM direktkandidatur dk
JOIN partei p ON dk.partei_id = p.id
JOIN wahlkreis wk ON dk.wahlkreis_id = wk.nummer
JOIN bundesland b ON wk.bundesland_id = b.id
WHERE dk.wahl_id = 21
GROUP BY p.id, p.name, b.id, b.name;

-- VIEW 6: Calculate total valid second votes nationwide (for divisor calculation)
CREATE OR REPLACE VIEW v_total_second_votes AS
SELECT COUNT(z.id) as total_votes
FROM zweitstimme z
JOIN v_qualifying_parties qp
    ON z.partei_id = qp.partei_id
JOIN wahlkreisergebnis we ON z.wahlkreisergebnis_id = we.id
WHERE gueltig = true AND we.wahl_id = 21;

-- Create materialized view to cache divisor calculation
DROP MATERIALIZED VIEW IF EXISTS mv_saintelague_divisor CASCADE;

CREATE MATERIALIZED VIEW mv_saintelague_divisor AS
WITH RECURSIVE divisor_iteration AS (
  -- Start with better initial divisor: total_votes / 630
  SELECT
    (SELECT total_votes::numeric FROM v_total_second_votes) / 630.0 as divisor,
    1 as iteration

  UNION ALL

  -- Recursive case: adjust divisor to converge to 630 seats
  SELECT
    CASE
      WHEN calc.seat_sum > 630 THEN (di.divisor * 1.002)::numeric
      WHEN calc.seat_sum < 630 THEN (di.divisor * 0.998)::numeric
      ELSE di.divisor
    END as divisor,
    di.iteration + 1
  FROM divisor_iteration di
  CROSS JOIN LATERAL (
    SELECT
      SUM(ROUND(nv.total_votes::numeric / di.divisor)) as seat_sum
    FROM v_national_votes nv
    WHERE nv.partei_id IN (SELECT partei_id FROM v_qualifying_parties)
  ) calc
  -- Stop after 20 iterations or when seat number is 630
  WHERE di.iteration < 20
    AND calc.seat_sum <> 630
),
final_result AS (
  SELECT divisor, iteration
  FROM divisor_iteration
  WHERE iteration = (SELECT MAX(iteration) FROM divisor_iteration)
  LIMIT 1
)
SELECT divisor, iteration
FROM final_result;

-- VIEW 7: STAGE 1 - National Sainte-Laguë allocation
-- Creates national seat allocation per party (not per bundesland)
CREATE OR REPLACE VIEW v_national_seat_allocation AS
SELECT
  nv.partei_id,
  nv.partei_name,
  nv.total_votes,
  nv.vote_share_percent,
  (nv.total_votes::numeric / (SELECT divisor FROM mv_saintelague_divisor)) as seat_value,
  ROUND(nv.total_votes::numeric / (SELECT divisor FROM mv_saintelague_divisor))::int as total_seats_allocated
FROM v_national_votes nv
WHERE nv.partei_id IN (SELECT partei_id FROM v_qualifying_parties)
ORDER BY total_seats_allocated DESC;

-- STAGE 2: For each party, find iterative divisor for bundesland distribution
-- Each party gets its own iterative divisor search for bundesland distribution
DROP MATERIALIZED VIEW IF EXISTS mv_party_bundesland_divisors CASCADE;

CREATE MATERIALIZED VIEW mv_party_bundesland_divisors AS
WITH party_targets AS (
  SELECT
    partei_id,
    total_seats_allocated as target_seats
  FROM v_national_seat_allocation
)
SELECT
  nsa.partei_id,
  nsa.partei_name,
  pt.target_seats,
  CASE
    WHEN pt.target_seats = 0 THEN 0
    ELSE (
      SELECT divisor FROM (
        WITH RECURSIVE divisor_iteration AS (
          -- Start with initial divisor estimate
          SELECT
            (SELECT SUM(second_votes)::numeric FROM v_party_bundesland_votes pbv WHERE pbv.partei_id = nsa.partei_id) /
            NULLIF(pt.target_seats, 0) as divisor,
            1 as iteration

          UNION ALL

          -- Adjust divisor until bundesland seats match target
          SELECT
            CASE
              WHEN calc.total_seats > pt.target_seats THEN (di.divisor * 1.002)::numeric
              WHEN calc.total_seats < pt.target_seats THEN (di.divisor * 0.998)::numeric
              ELSE di.divisor
            END as divisor,
            di.iteration + 1
          FROM divisor_iteration di
          CROSS JOIN LATERAL (
            SELECT SUM(ROUND(pbv.second_votes::numeric / di.divisor))::numeric as total_seats
            FROM v_party_bundesland_votes pbv
            WHERE pbv.partei_id = nsa.partei_id
          ) calc
          WHERE di.iteration < 50
            AND calc.total_seats <> pt.target_seats
        )
        SELECT divisor FROM divisor_iteration
        WHERE iteration = (SELECT MAX(iteration) FROM divisor_iteration)
        LIMIT 1
      ) final_divisor
    )
  END as divisor
FROM v_national_seat_allocation nsa
JOIN party_targets pt ON nsa.partei_id = pt.partei_id;

-- STAGE 2 Result: Bundesland distribution per party
CREATE OR REPLACE VIEW v_bundesland_seat_distribution AS
SELECT
  pb.partei_id,
  pb.partei_name,
  pb.bundesland_id,
  pb.bundesland_name,
  pb.second_votes,
  nsa.total_seats_allocated,
  ROUND(pb.second_votes::numeric / pbd.divisor)::int as seats_allocated
FROM v_party_bundesland_votes pb
JOIN v_national_seat_allocation nsa ON pb.partei_id = nsa.partei_id
JOIN mv_party_bundesland_divisors pbd ON pb.partei_id = pbd.partei_id
ORDER BY pb.partei_id, seats_allocated DESC;

-- VIEW 7: Direct mandate coverage check (Zweitstimmendeckung)
-- Now checks if direct mandate winners can get seats based on bundesland allocation
CREATE OR REPLACE VIEW v_mandate_coverage AS
SELECT
  COALESCE(bsd.partei_id, dm.partei_id) as partei_id,
  COALESCE(bsd.partei_name, dm.partei_name) as partei_name,
  COALESCE(bsd.bundesland_id, dm.bundesland_id) as bundesland_id,
  COALESCE(bsd.bundesland_name, dm.bundesland_name) as bundesland_name,
  COALESCE(bsd.seats_allocated, 0) as proportional_seats,
  COALESCE(dm.direct_mandate_count, 0) as direct_mandates_won,
  LEAST(COALESCE(bsd.seats_allocated, 0), COALESCE(dm.direct_mandate_count, 0)) as mandates_awarded,
  GREATEST(0, COALESCE(dm.direct_mandate_count, 0) - COALESCE(bsd.seats_allocated, 0)) as mandates_excluded
FROM v_bundesland_seat_distribution bsd
FULL OUTER JOIN v_direct_mandates dm
  ON bsd.partei_id = dm.partei_id
  AND bsd.bundesland_id = dm.bundesland_id;

-- VIEW 8: Final seat distribution (2025 method - no leveling seats)
-- Returns final allocated seats per party and bundesland
CREATE OR REPLACE VIEW v_final_seat_distribution AS
SELECT
  partei_id,
  partei_name,
  bundesland_id,
  bundesland_name,
  proportional_seats,
  mandates_awarded as direct_mandates,
  proportional_seats as total_seats
FROM v_mandate_coverage
WHERE proportional_seats > 0 OR mandates_awarded > 0
ORDER BY partei_id, bundesland_id;

-- VIEW 9: Summary statistics
CREATE OR REPLACE VIEW v_allocation_summary AS
SELECT
  (SELECT SUM(total_seats) FROM v_final_seat_distribution) as total_seats_distributed,
  (SELECT COUNT(DISTINCT partei_id) FROM v_final_seat_distribution) as parties_represented,
  (SELECT COUNT(DISTINCT bundesland_id) FROM v_final_seat_distribution) as bundeslands_affected,
  (SELECT COUNT(*) FROM v_qualifying_parties) as parties_qualifying,
  (SELECT total_votes FROM v_total_second_votes) as total_second_votes;

-- VIEW 10: National results summary
CREATE OR REPLACE VIEW v_national_summary AS
SELECT
  qp.partei_id,
  qp.partei_name,
  qp.total_votes,
  qp.vote_share_percent,
  COALESCE(SUM(fsd.total_seats), 0) as total_seats_nationwide
FROM v_qualifying_parties qp
LEFT JOIN v_final_seat_distribution fsd ON qp.partei_id = fsd.partei_id
GROUP BY qp.partei_id, qp.partei_name, qp.total_votes, qp.vote_share_percent
ORDER BY total_seats_nationwide DESC;

select * from v_national_summary;