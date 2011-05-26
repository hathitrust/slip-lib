
-----------------------------------------------------------------------
-- each shard producer updates the shard's row for EACH ITEM indexed.
-- there may be more than one producer per row/shard
-----------------------------------------------------------------------
CREATE TABLE `j_shard_stats` (
        `run`           smallint(3) NOT NULL default '0',
        `shard`         smallint(2) NOT NULL default '0',
        `s_num_docs`    int         NOT NULL default '0',
        `s_doc_size`    bigint      NOT NULL default '0',
        `s_doc_time`    float       NOT NULL default '0',
        `s_idx_time`    float       NOT NULL default '0',
        `s_tot_time`    float       NOT NULL default '0',
                PRIMARY KEY  (`run`, `shard`)
        );


CREATE TABLE `j_shard_stats` (`run` smallint(3) NOT NULL default '0', `shard` smallint(2) NOT NULL default '0', `s_num_docs` int NOT NULL default '0', `s_doc_size` bigint NOT NULL default '0', `s_doc_time` float NOT NULL default '0', `s_idx_time` float NOT NULL default '0', `s_tot_time` float NOT NULL default '0', PRIMARY KEY (`run`, `shard`));

-----------------------------------------------------------------------
-- recorded rates since last checkpoint
-- at intervals of 100, 1000, 10000, 100000
-----------------------------------------------------------------------
CREATE TABLE `j_rate_stats` (
        `run`           smallint(3) NOT NULL default '0',
        `shard`         smallint(2) NOT NULL default '0',
        `time_a_100`    int         NOT NULL default '0',
        `rate_a_100`    float       NOT NULL default '0',
                PRIMARY KEY  (`run`, `shard`)
        );

CREATE TABLE `j_rate_stats` (`run` smallint(3) NOT NULL default '0', `shard` smallint(2) NOT NULL default '0', `time_a_100` int NOT NULL default '0', `rate_a_100` float NOT NULL default '0', PRIMARY KEY (`run`, `shard`)); 


---------------------------------------------------------------------
--
---------------------------------------------------------------------

CREATE TABLE `j_errors` (
        `run`           smallint(3) NOT NULL default '0',
        `shard`         smallint(2) NOT NULL default '0',
        `id`            varchar(32) NOT NULL default '',
        `pid`           int         NOT NULL default '0',
        `host`          varchar(32) NOT NULL default '',
        `error_time`    timestamp   NOT NULL default '0000-00-00 00::00::00',
        `reason`        tinyint(1)  NULL,
                PRIMARY KEY  (`run`, `id`)
       );

CREATE TABLE `j_errors` (`run` smallint(3) NOT NULL default '0', `shard` smallint(2) NOT NULL default '0', `id` varchar(32) NOT NULL default '', `pid` int NOT NULL default '0', `host` varchar(32) NOT NULL default '', `error_time` timestamp NOT NULL default '0000-00-00 00::00::00', `reason` tinyint(1) NULL, PRIMARY KEY (`run`, `id`));

---------------------------------------------------------------------
--
---------------------------------------------------------------------

CREATE TABLE `j_timeouts` (
        `run`           smallint(3) NOT NULL default '0',
        `id`            varchar(32) NOT NULL default '',
        `shard`         smallint(2) NOT NULL default '0',
        `pid`           int         NOT NULL default '0',
        `host`          varchar(32) NOT NULL default '',
        `timeout_time`  timestamp   NOT NULL default '0000-00-00 00::00::00'
       );

CREATE TABLE `j_timeouts` (`run` smallint(3) NOT NULL default '0', `id` varchar(32) NOT NULL default '', `shard` smallint(2) NOT NULL default '0', `pid` int NOT NULL default '0', `host` varchar(32) NOT NULL default '', `timeout_time` timestamp NOT NULL default '0000-00-00 00::00::00');

---------------------------------------------------------------------
--
---------------------------------------------------------------------
CREATE TABLE `j_indexed` (
        `run`           smallint(3) NOT NULL default '0',
        `shard`         smallint(2) NOT NULL default '0',
        `id`            varchar(32) NOT NULL default '',
        `time`          timestamp   NOT NULL default '0000-00-00 00::00::00',
        `indexed_ct`    smallint(3) NOT NULL default '0',
                PRIMARY KEY    (`run`, `id`, `shard`),
                KEY `id`       (`id`),
                KEY `runshard` (`run`, `shard`),
                KEY `run`      (`run`),
                KEY `run_indexed_ct` (`run`, `indexed_ct`)
       );

CREATE TABLE `j_indexed` (`run` smallint(3) NOT NULL default '0', `shard` smallint(2) NOT NULL default '0', `id` varchar(32) NOT NULL default '', `time` timestamp NOT NULL default '0000-00-00 00::00::00', `indexed_ct` smallint(3) NOT NULL default '0', PRIMARY KEY (`run`, `id`, `shard`), KEY `id` (`id`), KEY `runshard` (`run`, `shard`), KEY `run` (`run`), KEY `run_indexed_ct` (`run`, `indexed_ct`));


---------------------------------------------------------------------
--
---------------------------------------------------------------------
CREATE TABLE `j_indexed_temp` (
        `shard`         smallint(2) NOT NULL default '0',
        `id`            varchar(32) NOT NULL default '',
                KEY `id` (`id`)
       );

CREATE TABLE `j_indexed_temp` (`shard` smallint(2) NOT NULL default '0', `id` varchar(32) NOT NULL default '', KEY `id` (`id`));

 
---------------------------------------------------------------------
-- No primary key. Useful to allows duplicates in queue. 
---------------------------------------------------------------------
CREATE TABLE `j_queue` (
        `run`           smallint(3) NOT NULL default '0',
        `id`            varchar(32) NOT NULL default '',
        `pid`           int         NOT NULL default '0',
        `host`          varchar(32) NOT NULL default '',
        `proc_status`   smallint(1) NOT NULL default '0',
                KEY `run` (`run`),
                KEY `id` (`id`),
                KEY `pid` (`pid`),
                KEY `host` (`host`),
                KEY `proc_status` (`proc_status`),
                KEY `runstatus` (`run`,`proc_status`)
       );


CREATE TABLE `j_queue` (`run` smallint(3) NOT NULL default '0', `id` varchar(32) NOT NULL default '', `pid` int NOT NULL default '0', `host` varchar(32) NOT NULL default '', `proc_status` smallint(1) NOT NULL default '0', KEY `run` (`run`), KEY `id` (`id`), KEY `pid` (`pid`), KEY `host` (`host`), KEY `proc_status` (`proc_status`), KEY `runstatus` (`run`,`proc_status`));

---------------------------------------------------------------------
-- No primary key. Useful to allows duplicates in queue. 
---------------------------------------------------------------------
CREATE TABLE `j_shared_queue` (
        `id`   varchar(32) NOT NULL default '',
        `time` timestamp   NOT NULL default CURRENT_TIMESTAMP,
             PRIMARY KEY `id` (`id`)
       );

CREATE TABLE `j_shared_queue` (`id` varchar(32) NOT NULL default '', `time` timestamp NOT NULL default CURRENT_TIMESTAMP, PRIMARY KEY `id` (`id`));


---------------------------------------------------------------------
-- Changes to j_rights MUST be made to j_rights_temp also!  
-- AND modify Db::initialize_j_rights_temp!
---------------------------------------------------------------------
CREATE TABLE `j_rights_temp` (
        `nid`         varchar(32) NOT NULL default '',
        `attr`        tinyint(4)  NOT NULL default '0',
        `reason`      tinyint(4)  NOT NULL default '0',
        `source`      tinyint(4)  NOT NULL default '0',
        `user`        varchar(32) NOT NULL default '',
        `time`        timestamp   NOT NULL default CURRENT_TIMESTAMP,
        `sysid`       varchar(32) NOT NULL default '',
        `update_time` int         NOT NULL default '00000000',
                PRIMARY KEY (`nid`),
                KEY `update_time` (`update_time`),
                KEY `attr` (`attr`)
        );

CREATE TABLE `j_rights_temp` (`nid` varchar(32) NOT NULL default '', `attr` tinyint(4) NOT NULL default '0', `reason` tinyint(4) NOT NULL default '0', `source` tinyint(4) NOT NULL default '0', `user` varchar(32) NOT NULL default '', `time` timestamp NOT NULL default CURRENT_TIMESTAMP, `sysid` varchar(32) NOT NULL default '', `update_time` int NOT NULL default '00000000', PRIMARY KEY (`nid`), KEY `update_time` (`update_time`), KEY `attr` (`attr`));

--

CREATE TABLE `j_rights` (
        `nid`         varchar(32) NOT NULL default '',
        `attr`        tinyint(4)  NOT NULL default '0',
        `reason`      tinyint(4)  NOT NULL default '0',
        `source`      tinyint(4)  NOT NULL default '0',
        `user`        varchar(32) NOT NULL default '',
        `time`        timestamp   NOT NULL default CURRENT_TIMESTAMP,
        `sysid`       varchar(32) NOT NULL default '',
        `update_time` int         NOT NULL default '00000000',
                PRIMARY KEY (`nid`),
                KEY `update_time` (`update_time`),
                KEY `attr` (`attr`)
        );

CREATE TABLE `j_rights` (`nid` varchar(32) NOT NULL default '', `attr` tinyint(4) NOT NULL default '0', `reason` tinyint(4) NOT NULL default '0', `source` tinyint(4) NOT NULL default '0', `user` varchar(32) NOT NULL default '', `time` timestamp NOT NULL default CURRENT_TIMESTAMP, `sysid` varchar(32) NOT NULL default '', `update_time` int NOT NULL default '00000000', PRIMARY KEY (`nid`), KEY `update_time` (`update_time`), KEY `attr` (`attr`));



---------------------------------------------------------------------
-- Pointer into VuFind Solr index
---------------------------------------------------------------------
CREATE TABLE `j_vsolr_timestamp` (
       `time` int NOT NULL default '00000000',
                PRIMARY KEY (`time`)
       );

CREATE TABLE `j_vsolr_timestamp` (`time` int NOT NULL default '00000000', PRIMARY KEY (`time`));


---------------------------------------------------------------------
-- Pointer into mdp.j_rights for the queue of each run
---------------------------------------------------------------------
CREATE TABLE `j_rights_timestamp` (
        `run`          smallint(3) NOT NULL default '0',
        `time`         int         NOT NULL default '00000000',
                PRIMARY KEY (`run`)
       );

CREATE TABLE `j_rights_timestamp` (`run` smallint(3) NOT NULL default '0', `time` int NOT NULL default '00000000', PRIMARY KEY (`run`));


---------------------------------------------------------------------
--
---------------------------------------------------------------------
CREATE TABLE `j_control` (
        `run`           smallint(3) NOT NULL default '0',
        `shard`         smallint(2) NOT NULL default '0',
        `host`          varchar(32) NOT NULL default '',
        `num_producers` smallint(2) NOT NULL default '1',
        `enabled`  tinyint(1)  NOT NULL default '0',
                PRIMARY KEY  (`run`, `shard`, `host`)
       );

CREATE TABLE `j_control` (`run` smallint(3) NOT NULL default '0', `shard` smallint(2) NOT NULL default '0', `host` varchar(32) NOT NULL default '', `num_producers` smallint(2) NOT NULL default '1', `enabled` tinyint(1) NOT NULL default '0', PRIMARY KEY  (`run`, `shard`, `host`));

---------------------------------------------------------------------
--
---------------------------------------------------------------------
CREATE TABLE `j_host_control` (
        `run`           smallint(3) NOT NULL default '0',
        `host`          varchar(32) NOT NULL default '',
        `num_producers` smallint(2) NOT NULL default '0',
        `enabled`  tinyint(1)  NOT NULL default '0',
                PRIMARY KEY (`run`, `host`)
       );

CREATE TABLE `j_host_control` (`run` smallint(3) NOT NULL default '0', `host` varchar(32) NOT NULL default '', `num_producers` smallint(2) NOT NULL default '0', `enabled`  tinyint(1) NOT NULL default '0', PRIMARY KEY (`run`, `host`));


---------------------------------------------------------------------
--
-- build    ::= 0=noerror                   2=error
-- optimiz  ::= 0=unoptimized, 1=optimized, 2=error
-- checkd   ::= 0=unchecked,   1=checked,   2=error
--
---------------------------------------------------------------------
CREATE TABLE `j_shard_control` (
        `run`           smallint(3) NOT NULL default '0',
        `shard`         smallint(2) NOT NULL default '0',
        `enabled`       tinyint(1)  NOT NULL default '0',
        `suspended`     tinyint(1)  NOT NULL default '0',
        `build`         tinyint(1)  NOT NULL default '0',
        `optimiz`       tinyint(1)  NOT NULL default '0',
        `checkd`        tinyint(1)  NOT NULL default '0',
        `build_time`    timestamp   NOT NULL default '0000-00-00 00::00::00',
        `optimize_time` timestamp   NOT NULL default '0000-00-00 00::00::00',
        `checkd_time`   timestamp   NOT NULL default '0000-00-00 00::00::00',
        `release_state` tinyint(1)  NOT NULL default '0',
                PRIMARY KEY  (`run`, `shard`)
       );

CREATE TABLE `j_shard_control` (`run`  smallint(3) NOT NULL default '0', `shard` smallint(2) NOT NULL default '0', `enabled` tinyint(1) NOT NULL default '0', `suspended` tinyint(1) NOT NULL default '0', `build` tinyint(1) NOT NULL default '0', `optimiz` tinyint(1) NOT NULL default '0', `checkd` tinyint(1) NOT NULL default '0', `build_time` timestamp NOT NULL default '0000-00-00 00::00::00', `optimize_time` timestamp NOT NULL default '0000-00-00 00::00::00', `checkd_time` timestamp NOT NULL default '0000-00-00 00::00::00', `release_state` tinyint(1) NOT NULL default '0', PRIMARY KEY (`run`, `shard`));


---------------------------------------------------------------------
--
---------------------------------------------------------------------
CREATE TABLE `j_driver_control` (
        `run`           smallint(3) NOT NULL default '0',
        `enabled`       tinyint(1)  NOT NULL default '0',
        `stage`         varchar(32) NOT NULL default 'Undefined',
                PRIMARY KEY  (`run`)
       );

CREATE TABLE `j_driver_control` (`run` smallint(3) NOT NULL default '0', `enabled` tinyint(1) NOT NULL default '0', `stage` varchar(32) NOT NULL default 'Undefined', PRIMARY KEY (`run`));



---------------------------------------------------------------------
--
---------------------------------------------------------------------
CREATE TABLE `j_enqueuer_control` (
        `run`      smallint(3) NOT NULL default '0',
        `enabled`  tinyint(1)  NOT NULL default '0',
                PRIMARY KEY  (`run`)
       );

CREATE TABLE `j_enqueuer_control` (`run` smallint(3) NOT NULL default '0', `enabled` tinyint(1) NOT NULL default '0', PRIMARY KEY (`run`));


---------------------------------------------------------------------
--
---------------------------------------------------------------------
CREATE TABLE `j_commit_control` (
        `run`      smallint(3) NOT NULL default '0',
        `shard`    smallint(2) NOT NULL default '0',
        `enabled`  tinyint(1)  NOT NULL default '0',
                PRIMARY KEY  (`run`, `shard`)
       );

CREATE TABLE `j_commit_control` (`run` smallint(3) NOT NULL default '0', `shard` smallint(2) NOT NULL default '0', `enabled` tinyint(1) NOT NULL default '0', PRIMARY KEY (`run`, `shard`));

---------------------------------------------------------------------
--
---------------------------------------------------------------------
CREATE TABLE `j_check_control` (
        `run`      smallint(3) NOT NULL default '0',
        `shard`    smallint(2) NOT NULL default '0',
        `enabled`  tinyint(1)  NOT NULL default '0',
                PRIMARY KEY  (`run`, `shard`)
       );

CREATE TABLE `j_check_control` (`run` smallint(3) NOT NULL default '0', `shard` smallint(2) NOT NULL default '0', `enabled` tinyint(1) NOT NULL default '0', PRIMARY KEY (`run`, `shard`));


---------------------------------------------------------------------
--
---------------------------------------------------------------------
CREATE TABLE `j_rights_control` (
        `enabled`  tinyint(1)  NOT NULL default '0'
       );

CREATE TABLE `j_rights_control` (`enabled` tinyint(1) NOT NULL default '0');


---------------------------------------------------------------------
-- Do a du in bytes for the run for a given shard
---------------------------------------------------------------------
CREATE TABLE `j_index_size` (
        `run`   smallint(3) NOT NULL default '0',
        `shard` smallint(2) NOT NULL default '0',
        `du`    bigint(20)  NOT NULL default '0',
                PRIMARY KEY  (`run`, `shard`)
       );

CREATE TABLE `j_index_size` (`run` smallint(3) NOT NULL default '0', `shard` smallint(2) NOT NULL default '0', `du` bigint(20) NOT NULL default '0', PRIMARY KEY (`run`, `shard`));
