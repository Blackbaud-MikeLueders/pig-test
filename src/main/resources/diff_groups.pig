-- Clear Outputs
rmf $OUTPUT/members_to_remove.gz
rmf $OUTPUT/members_to_add.gz

-- Process Inputs
source = LOAD '$INPUT/source_group.gz' USING PigStorage('\n') AS source_member: long;
target = LOAD '$INPUT/target_group.gz' USING PigStorage('\n') AS target_member: long;

-- Combine Data
combined = JOIN source BY source_member FULL OUTER, target BY target_member;

-- Output Data
SPLIT combined INTO member_to_add IF source_member IS NULL,
                    member_to_remove IF target_member IS NULL;

members_to_add = FOREACH member_to_add GENERATE target_member;
members_to_remove = FOREACH member_to_remove GENERATE source_member;

STORE members_to_add INTO '$OUTPUT/members_to_add.gz' USING PigStorage();
STORE members_to_remove INTO '$OUTPUT/members_to_remove.gz' USING PigStorage();
