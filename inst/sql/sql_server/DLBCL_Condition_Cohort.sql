CREATE TABLE #Codesets (
  codeset_id int NOT NULL,
  concept_id bigint NOT NULL
)
;

INSERT INTO #Codesets (codeset_id, concept_id)
SELECT 0 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (3255258)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (3255258)
  and c.invalid_reason is null
UNION
select distinct cr.concept_id_1 as concept_id
FROM
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (3255258)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (3255258)
  and c.invalid_reason is null

) C
join @vocabulary_database_schema.concept_relationship cr on C.concept_id = cr.concept_id_2 and cr.relationship_id = 'Maps to' and cr.invalid_reason IS NULL

) I
) C UNION ALL 
SELECT 1 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (21037861,21057492,21175294,21165580,35148549,35139293,41332949,43860477,35777943,35776276,40718524,35776000,40718523,43680447,43752133,43644428,41332592,41333275,41328140,43770172,35137425,45071349,19074494,45249894,45315179,43275156,19040302,43291307,35777583,35777275,21126079,21096733,21106577,21145806,42480231,21067337,42480806,21165581,21067338,21057493,42922498,42922499,41328944,43788027,35777075,40718526,35776838,35777687,40718525,43734233,43644427,43842294,41337289,41327948,41334169,43860476,19074495,43291308,36895158,43296714,35778150,35776159,42482576,21135943,21106578,21096734,21086974,21037862,45279230,35601619,45128534,45253892,45117232,45373152,21078174,21166560,21038888,43823665,43769624,43859937,43807742,43789635,43591859,44210277,44210276,44210287,44210438,44210448,21059226,43198075,21167306,43165133,41513330,41511925,41515094,41513329,43142969,35421089,41515825,41513665,41516566,42876778,37594211,21059227,43793028,21167307,21147573,40703964,21137731,21088621,44212214,44210812,44213890,44215281,44216110,35797356,44214841,35797371,44215118,44214630,44215341,44211218,44216057,44214629,44213223,44216056,44215340,37594222,36414578,35797493,35797919,35797918,35797565,40704258,35797566,43045285,43045286,43045287,43045288,43045289,43045290,41481188,41479843,41481233,41480507,41480300,43682485,41480568,41481234,41480268,41480893,43682486,43754271,41481825,41483671,41482006,41482969,41481760,41481941,41482913,41482970,41483103,41483635,21078493,43219427,21058805,43186529,41491178,41491367,41493085,41491745,43208414,35420541,41490769,41491177,41490996,37594173,43665265,21049044,43044938,43044939,43044940,43045141,43045142,43045143,43045144,43045147,43045148,43045145,43045146,43045137,43045139,43045140,43045138,44210062,44209454,44209877,44209742,44210133,35420939,35420871,35421011,35420914,41506132,41509755,41506862,41510842,35420777,35420776,41506131,41510519,41506861,37594200,35797141,35797059,36414196,35797058,40704454,35796808,35796890,35507077,35519592,42958478,42958479,42958475,42958476,42958477,43029037,43029038,43029039,43029041,43029040,43029035,43029036,43029047,43029048,43029049,43029051,43029050,43029045,43029046,43029044,43029043,43029042,45621833,4286716,45918571,4280262,44523429,44353698,45679635,36157955,45701561,45632963,1356649,45654745,44340381,45681193,44356436,46303007,36153196,36151385,42805453,36983408,44343910,1721804,36155639,44341740,44344228,44350946,45678894,45644656,45707432,45701002,44342016,45894869,1370597,46305926,44357342,45626815,45645552,35600062,45685880,45632130,45662093,46314298,45683378,36491312,36484592,44350783,37315221,42642769,45637357,36000091,35302619,45647378,44615594,45081665,44894574,44585161,44911627,45126859,45263429,45271296,45714062,42855752,4277397,42548512,45727871,42548513,4277398,42855753,45725090,45729253,45712037,45115669,45217999,580524,580381,4277723,42548514,42855754,42857199,42553838,4323255,45722303,45729254,4278491,42548515,42855755,44553969,45712518,42859477,40201969,44563797,44642738,44594100,44641166,45325763,44523430,576959,37042242,3031700,37038549,45885335,40789188,4348074,3192294,21178714,4123687,21290779,45634367,35167006,45643674,44145126,45652001,45650520,19035631,45652719,37487380,21234413,43045929,43228194,43314887,45957980,45658672,4195394,43239911,43317677,43308803,43353871,43353872,43317678,43308804,43335876,43317679,43299768,43362873,43353873,43353874,43335877,43344830,43326837,43335878,43344831,43317680,43344832,43317681,43353875,43326838,45148907,44928720,45199963,45251380,45892758,44168167,44172707,44172706,44181397,44173916,44164442,19035667,41118533,40993513,40880329,40880328,19035666,43151418,41192670,41098758,21159422,43162603,21021856,21031605,44115952,21139624,21129722,40743476,21090467,40167961,45229126,45331425,41142241,21100401,40167962,19035668,19035669,45382571,19035670,19045141,43029029,43029031,43029032,43029033,43029034,43029030,21139618,21159410,21110211,21129715,21061086,19035662,44411941,593598,44943111,19074492,45355954,42832207,42832208,44439107,45128731,45100456,45143158,45126724,45259336,45241740,44890587,45361692,44941757,36183031,45075609,44447309,45344531,44912924,45065339,45142676,45254374,45297762,21129718,21071030,21061087,43265860,43276909,21051298,43260430,40743471,21110213,21080716,40899894,21149478,21021854,21159413,21080717,40743472,21031600,21080718,40743473,21129717,21139620,21051297,41305448,41087231,41055748,41192646,41317039,41262063,41254676,41043475,40887807,40880311,43271350,43271351,41004968,41223705,40981088,41004967,41012371,40849132,41012370,43658474,43730197,41317038,41262061,40880310,41231331,40849131,41262062,43676543,43586137,40911341,41137683,41004966,41012369,41192645,41106269,43622195,41285705,41285704,41200258,41130121,41012368,40942618,41168892,43730198,43640351,43676544,44086949,44064505,35134879,43293084,43271349,43802105,41067264,41324582,41231330,40973580,40887806,41106268,41317037,41293228,40981087,43856339,35130566,43282221,43255085,43838327,41098733,40856526,41067263,41231329,41317036,40856527,43604291,35144835,43586138,40911340,41262060,41098732,40887805,41223704,40949997,43820230,35129433,19040280,45068029,44935004,44873345,45230192,45076668,45859359,44957206,44963016,45118893,45306822,21110214,21100400,21090465,21100399,21041489,40743469,21119936,21139621,21031602,43151419,40743468,21159414,21041488,21031601,44051532,21129719,40743470,21051299,21071031,40849134,40911342,44179133,41231325,40981086,41106270,40918703,41043476,41168890,40981085,41043473,41074832,41168889,41200257,40856524,41074831,41168888,41074830,41262059,40849133,41231328,41231327,41106267,41106271,40918705,41043474,40949998,40856525,41012367,40918704,41074833,41231326,41106266,41168891,41106265,41223703,41254675,36508708,43293085,43255086,43255084,43265861,35135077,35158930,21139619,21110212,43255083,35139201,21129716,43293083,35155780,40743474,21031599,21080715,43271348,35146834,40743475,21159412,21100398,21169189,35131730,35136517,35142535,41110787,43282220,41016717,43802104,43622194,41016716,43820235,43694342,44080873,35156132,35155654,43255082,43784236,35139643,43271347,43856338,35142277,43676542,35139950,19022683,40860984,43265859,43260429,35146363,35130310,42958480,42958481,42958482,42958483,42958484,42958485,42958486,42958487,42958488,42958489,19035663,44986297,44441920,44986298,45196493,45071223,45335183,44418154,19074493,44839224,44418116,45229600,45003293,36480459,45231466,44433080,36183032,44870457,45282690,21159419,21071032,21080719,21080720,40743481,21110217,40899895,21149479,21021855,21041490,21159418,40743482,21139622,21169190,40743483,21159417,21129720,21031604,41087232,41181082,40868766,41098734,40911345,40950000,41043481,40880313,41137687,41324588,41285708,40942623,41254680,41293233,41192649,41200260,41067266,41106276,43622190,43820231,41223708,40856530,41254679,41200259,41192648,40949999,43820232,43784234,41285707,40918706,41223707,41231334,40880312,41106275,43820233,40973586,40942622,41324587,40973585,41043480,41035999,40856529,43658473,43694340,43730200,44074066,44064506,35141809,35151742,43694339,40911344,41324585,41106273,40973584,41262067,41106274,40973583,41324586,40856528,43604290,35135109,43586136,41067265,41137686,40973582,41012372,40849135,40887816,43838326,35133063,43784235,41161496,40887814,41285706,41137685,40973581,40887815,43658472,35137440,45264361,19040301,44852092,45248386,45056497,45116433,45161546,45255643,21051300,21159420,40743479,21159421,36258123,36274139,44129118,21129721,40743480,21090466,40911343,40942621,44182883,41262066,41043477,41293229,41074836,41262065,41324583,41262064,41043479,41293231,41074835,40887810,41231333,40887809,41074834,41192647,41231332,40887813,40887812,41043478,41293230,40887808,41106272,41137684,41324584,41293232,41168894,40887811,41168893,41254678,40942620,35158205,35144980,21159416,21110215,35149886,21061089,35139790,40743484,21119937,21110216,35134733,40743485,21159415,21031603,21061088,35141016,41173278,41204818,43712167,43730199,41016718,43766093,43784233,44042131,35144076,35150785,43640350,35149691,43838325,35147954,43676545,35140869,19022684,41079403,35145864,35145484,35159369,35158841,35134313,35136781,19035664,45091431,45228410,36881139,44870610,44937933,44960639,45124889,45133183,45280264,43622196,35775112,35746326,45077344,44976671,40743467,45145011,35763044,40743466,43820234,43712168,41254677,41317040,35158554,35605187,44084820,43260428,35605188,44099940,44051533,43287656,44051534,44064507,44084821,44112758,45178567,19035661,44855162,19035636,21080714,44090139,21149477,42479056,21100397,42479664,21159411,42482040,21090464,19021165,43766094,41266652,19120658,42958490,42958491,42958492,42958493,19035665,45892759,45902471,36884938,44869554,44875627,44959956,44990022,45227555,43622191,35745746,35771477,19040303,40743478,45026155,45264932,35763043,40743477,43622192,43622193,41223706,40942619,35605189,44071961,43287657,36889028,44112757,44077243,43287658,44103166,44097785,44035177,42482039,21139623,21071033,21110218,19021166,21080721,43694341,41048032,43333051,43369092,3066075,40551700,40556095,3152605,35167009,35167014,35167011,35167007,35167010,35167008,35167012,35167013,35167015,35167016,35167017,35167018,35167019,35167020,35167021,35167022,3060649,40551699,40554657,3152490,35167023,35167027,35167024,35167029,35167028,35167026,35167025,35167030,35167031,35167032,35167033,35167034,35167035,35167036,35167037,35167038,35167039,43305977,43305978,4131903,36683714,35626315,44876415,45063706,3062718,40555178,3148911,3190062,36815407,36691440,4210658,45974663,21282072,46100394,21292766,21200103,21184474,21311090,45971262,46025311,21267807,46163085,46085315,21280593,21225855,46095912,35167040,35167041,43333052,43351003,36683715,35626316,44927534,21185665,21274283,21219213,21181708,21241271,21314708,21184901,4212671,21296366,21202989,45979738,21230306,46158121,21301815,46111001,46018998,46156095,21323358,21323357,46156094,46040415,46180167,21305989,46017106,21285926,46154029,46154028,21192896,46033160,46171815,21286893,46022295,46159655,21323519,21323518,46159654,45979739,21245846,46111002,46047829,46189042,21306489,46102734,21319614,21206256,46086237,21319615,46102736,3152545,40555183,40549825,3063774,45974672,4219957,36691441,36815408,21319362,46100384,3148912,21181239,21311433,4236617,21181253,21199859,40570380,3156923,45974784,21189151,46101944,46101945,21263647,21237965,4210657,21296099,21259437,21296367,21314709,21199803,21256569,21222756,21185666,45982069,46158123,21304774,46113760,21190274,46019000,46156098,21248923,21211725,46156097,46040416,21194306,46180168,46017104,21230092,46154026,46154025,21248769,46033162,46171816,21212539,46022296,46159657,21304840,21249075,46159656,45982070,46113761,21320620,46047830,21194790,46189043,21207726,46101295,21319469,46101297,45966773,21276918,21310577,46012128,46148429,21304170,46088938,21243825,45966772,21198876,21239726,46012127,21211170,46148428,46088940,21280951,21184019,45965003,21291706,46015715,21323104,46152410,46086985,21206311,45965002,21217361,21240030,46015714,21285834,46152409,46086986,21299574,40802694,43564652,36216223,40018630,36885623,42958499,40743486,35766275,40018631,43676541,41300871,42958500,35148637,35605185,44548212,44654009,35605186,44683509,44030093,44083351,43564653,40802695,40018632,42958494,40833055,42958495,42958496,43271346,41082706,43766092,43802103,41019902,43640348,43640349,44025874,35141742,35129815,43293082,43748121,35131950,43265858,42958497,43622189,35129477,42958498,43712166,35148177,40018633,40864262,43260427,43282219,35153704,35152551,36216224,36218176,40018634,40018635,42482038,35149488,35129833,36218177,42479055,42483183,40830816,40833059,40018636,40926429,21169188,40157368,36218178,3014173,40760586,40760582,40760584,40760579,3011391,40760585,40760581,40760583,40760580,35167043,35167042,4203224,36677581,36681036,36681037,36681038,40439077,40776774,37063834,37071916,45411801,45416662,45416663,45410907,45411802,43564654,43564655,44014732,44150247,44600476,44596135,36675363,4203232,4235684,1020794,37079670,1020795,37079671,1020796,37079672,1020797,37079673,40654490,37488412,35173839,35173840,35173841,35173842,35173843,2718287,40217597,36161288,1720729,44394902,1170878,36475884,45802718,3210954,3212732,3209968,3207777,3203193,3203569,3210083,3212305,45646261,45642403,45652273,45656505,4354237,45706350,21600819,35167005,36296559,21602040,45370909,45217180,36190057,42881226,45060011,44398408,45846392,45634354,45851632,45685000,45854339,45789058,46324126,45820938,42822190,1528382,44486016,45785196,45853972,44474593,1357136,1357906,44402164,45798504,45813622,45821011,45367729,45674392,42839184,36168052,44467403,36193712,44467727,36989413,44479355,44396461,44935552,35514831,1723321,36189276,36996238,44474721,36996237,1767125,45811836,36194228,45899579,36996239,42817813,44883893,45111495,45097802,36172433,42652239,36917788,44404329,36463511,45668786,592813,1722500,36465815,36170401,44373000,36987867,35517429,42652181,36161040,44375409,36174316,45659156,45781392,45793663,45800384,46239711,45628437,45627852,44486019,36190058,45846393,45299780,45813623,36189277,36996241,36195641,44491234,36996242,45077481,44467728,36194229,45811837,44399279,35903617,44402489,44951929,1723322,45207956,36989414,45899580,44467404,36996240,45290126,45146395,45163591,36176703,1565861,36179649,36180242,36180243,44465349,44434960,44982659,45299352,45380605,46252940,46249218,1565862,42824005,44413107,44879124,44906956,44904278,45178972,45023194,45869976,45863747,45901948,45903280,1358038,1723745,1723746,44428155,36176702,44460979,44408594,44422972,44428041,44446149,44997985,44845184,45117757,750369,45340962,44411779,35905435,44425290,36197064,45254093,44451320,36176704,45296318,36180245,1723747,36197063,45134250,36180244,36197062,44425395,45900608,46255179,45356790,35304604,36001711,45366903,45381423,44845185,44471815,44400627,42368118,42295931,42368119,42368116,42368117,42383227,45302736,45690731,45071348,36175230,42809499,36476825,36173661,45675843,36476882,35601363,35600520,44839026,45810374,45685537,36474971,45800429,45810381,45846798,45045218,45689681,45780740,45282054,45778259,35303169,36001336,45620236,1719870,45846394,35304253,45366902,36000673,44397911,45813637,42368120,42368121,45810382,45131969,45695187,1371925,45822377,45897973,45249131,45897343,46325933,45801914,36171485,1566414,36494919,45849086,1564751,44485126,45849087,44401499,45409852,45857125,45787709,35173838,35197846,35140880,35149022,35150053,35130186,35151701,35150929,35156017,35155321,35152521,35142840,35143138,35129421,35156987,35134354,35131587)

) I
) C UNION ALL 
SELECT 2 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (42530708)

) I
) C UNION ALL 
SELECT 3 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @vocabulary_database_schema.CONCEPT where 0=1
) I
LEFT JOIN
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (44808122)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (44808122)
  and c.invalid_reason is null
UNION
select distinct cr.concept_id_1 as concept_id
FROM
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (44808122)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (44808122)
  and c.invalid_reason is null

) C
join @vocabulary_database_schema.concept_relationship cr on C.concept_id = cr.concept_id_2 and cr.relationship_id = 'Maps to' and cr.invalid_reason IS NULL

) E ON I.concept_id = E.concept_id
WHERE E.concept_id is null
) C
;

with primary_events (event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id) as
(
-- Begin Primary Events
select P.ordinal as event_id, P.person_id, P.start_date, P.end_date, op_start_date, op_end_date, cast(P.visit_occurrence_id as bigint) as visit_occurrence_id
FROM
(
  select E.person_id, E.start_date, E.end_date,
         row_number() OVER (PARTITION BY E.person_id ORDER BY E.sort_date ASC) ordinal,
         OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date, cast(E.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM 
  (
  -- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, DATEADD(day,1,C.condition_start_date)) as end_date,
  C.visit_occurrence_id, C.condition_start_date as sort_date
FROM 
(
  SELECT co.* 
  FROM @cdm_database_schema.CONDITION_OCCURRENCE co
  JOIN #Codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 3)
) C


-- End Condition Occurrence Criteria

  ) E
	JOIN @cdm_database_schema.observation_period OP on E.person_id = OP.person_id and E.start_date >=  OP.observation_period_start_date and E.start_date <= op.observation_period_end_date
  WHERE DATEADD(day,1,OP.OBSERVATION_PERIOD_START_DATE) <= E.START_DATE AND DATEADD(day,1095,E.START_DATE) <= OP.OBSERVATION_PERIOD_END_DATE
) P
WHERE P.ordinal = 1
-- End Primary Events

)
SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id
INTO #qualified_events
FROM 
(
  select pe.event_id, pe.person_id, pe.start_date, pe.end_date, pe.op_start_date, pe.op_end_date, row_number() over (partition by pe.person_id order by pe.start_date ASC) as ordinal, cast(pe.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM primary_events pe
  
) QE

;

--- Inclusion Rule Inserts

create table #inclusion_events (inclusion_rule_id bigint,
	person_id bigint,
	event_id bigint
);

with cteIncludedEvents(event_id, person_id, start_date, end_date, op_start_date, op_end_date, ordinal) as
(
  SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, row_number() over (partition by person_id order by start_date ASC) as ordinal
  from
  (
    select Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date, SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) as inclusion_rule_mask
    from #qualified_events Q
    LEFT JOIN #inclusion_events I on I.person_id = Q.person_id and I.event_id = Q.event_id
    GROUP BY Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date
  ) MG -- matching groups
{0 != 0}?{
  -- the matching group with all bits set ( POWER(2,# of inclusion rules) - 1 = inclusion_rule_mask
  WHERE (MG.inclusion_rule_mask = POWER(cast(2 as bigint),0)-1)
}
)
select event_id, person_id, start_date, end_date, op_start_date, op_end_date
into #included_events
FROM cteIncludedEvents Results
WHERE Results.ordinal = 1
;



-- generate cohort periods into #final_cohort
with cohort_ends (event_id, person_id, end_date) as
(
	-- cohort exit dates
  -- By default, cohort exit at the event's op end date
select event_id, person_id, op_end_date as end_date from #included_events
),
first_ends (person_id, start_date, end_date) as
(
	select F.person_id, F.start_date, F.end_date
	FROM (
	  select I.event_id, I.person_id, I.start_date, E.end_date, row_number() over (partition by I.person_id, I.event_id order by E.end_date) as ordinal 
	  from #included_events I
	  join cohort_ends E on I.event_id = E.event_id and I.person_id = E.person_id and E.end_date >= I.start_date
	) F
	WHERE F.ordinal = 1
)
select person_id, start_date, end_date
INTO #cohort_rows
from first_ends;

with cteEndDates (person_id, end_date) AS -- the magic
(	
	SELECT
		person_id
		, DATEADD(day,-1 * 0, event_date)  as end_date
	FROM
	(
		SELECT
			person_id
			, event_date
			, event_type
			, MAX(start_ordinal) OVER (PARTITION BY person_id ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS start_ordinal 
			, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY event_date, event_type) AS overall_ord
		FROM
		(
			SELECT
				person_id
				, start_date AS event_date
				, -1 AS event_type
				, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY start_date) AS start_ordinal
			FROM #cohort_rows
		
			UNION ALL
		

			SELECT
				person_id
				, DATEADD(day,0,end_date) as end_date
				, 1 AS event_type
				, NULL
			FROM #cohort_rows
		) RAWDATA
	) e
	WHERE (2 * e.start_ordinal) - e.overall_ord = 0
),
cteEnds (person_id, start_date, end_date) AS
(
	SELECT
		 c.person_id
		, c.start_date
		, MIN(e.end_date) AS end_date
	FROM #cohort_rows c
	JOIN cteEndDates e ON c.person_id = e.person_id AND e.end_date >= c.start_date
	GROUP BY c.person_id, c.start_date
)
select person_id, min(start_date) as start_date, end_date
into #final_cohort
from cteEnds
group by person_id, end_date
;

DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;
INSERT INTO @target_database_schema.@target_cohort_table (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
select @target_cohort_id as cohort_definition_id, person_id, start_date, end_date 
FROM #final_cohort CO
;

{0 != 0}?{
-- BEGIN: Censored Stats

delete from @results_database_schema.cohort_censor_stats where cohort_definition_id = @target_cohort_id;

-- END: Censored Stats
}
{0 != 0 & 0 != 0}?{

CREATE TABLE #inclusion_rules (rule_sequence int);

-- Find the event that is the 'best match' per person.  
-- the 'best match' is defined as the event that satisfies the most inclusion rules.
-- ties are solved by choosing the event that matches the earliest inclusion rule, and then earliest.

select q.person_id, q.event_id
into #best_events
from #qualified_events Q
join (
	SELECT R.person_id, R.event_id, ROW_NUMBER() OVER (PARTITION BY R.person_id ORDER BY R.rule_count DESC,R.min_rule_id ASC, R.start_date ASC) AS rank_value
	FROM (
		SELECT Q.person_id, Q.event_id, COALESCE(COUNT(DISTINCT I.inclusion_rule_id), 0) AS rule_count, COALESCE(MIN(I.inclusion_rule_id), 0) AS min_rule_id, Q.start_date
		FROM #qualified_events Q
		LEFT JOIN #inclusion_events I ON q.person_id = i.person_id AND q.event_id = i.event_id
		GROUP BY Q.person_id, Q.event_id, Q.start_date
	) R
) ranked on Q.person_id = ranked.person_id and Q.event_id = ranked.event_id
WHERE ranked.rank_value = 1
;

-- modes of generation: (the same tables store the results for the different modes, identified by the mode_id column)
-- 0: all events
-- 1: best event


-- BEGIN: Inclusion Impact Analysis - event
-- calculte matching group counts
delete from @results_database_schema.cohort_inclusion_result where cohort_definition_id = @target_cohort_id and mode_id = 0;
insert into @results_database_schema.cohort_inclusion_result (cohort_definition_id, inclusion_rule_mask, person_count, mode_id)
select @target_cohort_id as cohort_definition_id, inclusion_rule_mask, count_big(*) as person_count, 0 as mode_id
from
(
  select Q.person_id, Q.event_id, CAST(SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) AS bigint) as inclusion_rule_mask
  from #qualified_events Q
  LEFT JOIN #inclusion_events I on q.person_id = i.person_id and q.event_id = i.event_id
  GROUP BY Q.person_id, Q.event_id
) MG -- matching groups
group by inclusion_rule_mask
;

-- calculate gain counts 
delete from @results_database_schema.cohort_inclusion_stats where cohort_definition_id = @target_cohort_id and mode_id = 0;
insert into @results_database_schema.cohort_inclusion_stats (cohort_definition_id, rule_sequence, person_count, gain_count, person_total, mode_id)
select @target_cohort_id as cohort_definition_id, ir.rule_sequence, coalesce(T.person_count, 0) as person_count, coalesce(SR.person_count, 0) gain_count, EventTotal.total, 0 as mode_id
from #inclusion_rules ir
left join
(
  select i.inclusion_rule_id, count_big(i.event_id) as person_count
  from #qualified_events Q
  JOIN #inclusion_events i on Q.person_id = I.person_id and Q.event_id = i.event_id
  group by i.inclusion_rule_id
) T on ir.rule_sequence = T.inclusion_rule_id
CROSS JOIN (select count(*) as total_rules from #inclusion_rules) RuleTotal
CROSS JOIN (select count_big(event_id) as total from #qualified_events) EventTotal
LEFT JOIN @results_database_schema.cohort_inclusion_result SR on SR.mode_id = 0 AND SR.cohort_definition_id = @target_cohort_id AND (POWER(cast(2 as bigint),RuleTotal.total_rules) - POWER(cast(2 as bigint),ir.rule_sequence) - 1) = SR.inclusion_rule_mask -- POWER(2,rule count) - POWER(2,rule sequence) - 1 is the mask for 'all except this rule'
;

-- calculate totals
delete from @results_database_schema.cohort_summary_stats where cohort_definition_id = @target_cohort_id and mode_id = 0;
insert into @results_database_schema.cohort_summary_stats (cohort_definition_id, base_count, final_count, mode_id)
select @target_cohort_id as cohort_definition_id, PC.total as person_count, coalesce(FC.total, 0) as final_count, 0 as mode_id
FROM
(select count_big(event_id) as total from #qualified_events) PC,
(select sum(sr.person_count) as total
  from @results_database_schema.cohort_inclusion_result sr
  CROSS JOIN (select count(*) as total_rules from #inclusion_rules) RuleTotal
  where sr.mode_id = 0 and sr.cohort_definition_id = @target_cohort_id and sr.inclusion_rule_mask = POWER(cast(2 as bigint),RuleTotal.total_rules)-1
) FC
;

-- END: Inclusion Impact Analysis - event

-- BEGIN: Inclusion Impact Analysis - person
-- calculte matching group counts
delete from @results_database_schema.cohort_inclusion_result where cohort_definition_id = @target_cohort_id and mode_id = 1;
insert into @results_database_schema.cohort_inclusion_result (cohort_definition_id, inclusion_rule_mask, person_count, mode_id)
select @target_cohort_id as cohort_definition_id, inclusion_rule_mask, count_big(*) as person_count, 1 as mode_id
from
(
  select Q.person_id, Q.event_id, CAST(SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) AS bigint) as inclusion_rule_mask
  from #best_events Q
  LEFT JOIN #inclusion_events I on q.person_id = i.person_id and q.event_id = i.event_id
  GROUP BY Q.person_id, Q.event_id
) MG -- matching groups
group by inclusion_rule_mask
;

-- calculate gain counts 
delete from @results_database_schema.cohort_inclusion_stats where cohort_definition_id = @target_cohort_id and mode_id = 1;
insert into @results_database_schema.cohort_inclusion_stats (cohort_definition_id, rule_sequence, person_count, gain_count, person_total, mode_id)
select @target_cohort_id as cohort_definition_id, ir.rule_sequence, coalesce(T.person_count, 0) as person_count, coalesce(SR.person_count, 0) gain_count, EventTotal.total, 1 as mode_id
from #inclusion_rules ir
left join
(
  select i.inclusion_rule_id, count_big(i.event_id) as person_count
  from #best_events Q
  JOIN #inclusion_events i on Q.person_id = I.person_id and Q.event_id = i.event_id
  group by i.inclusion_rule_id
) T on ir.rule_sequence = T.inclusion_rule_id
CROSS JOIN (select count(*) as total_rules from #inclusion_rules) RuleTotal
CROSS JOIN (select count_big(event_id) as total from #best_events) EventTotal
LEFT JOIN @results_database_schema.cohort_inclusion_result SR on SR.mode_id = 1 AND SR.cohort_definition_id = @target_cohort_id AND (POWER(cast(2 as bigint),RuleTotal.total_rules) - POWER(cast(2 as bigint),ir.rule_sequence) - 1) = SR.inclusion_rule_mask -- POWER(2,rule count) - POWER(2,rule sequence) - 1 is the mask for 'all except this rule'
;

-- calculate totals
delete from @results_database_schema.cohort_summary_stats where cohort_definition_id = @target_cohort_id and mode_id = 1;
insert into @results_database_schema.cohort_summary_stats (cohort_definition_id, base_count, final_count, mode_id)
select @target_cohort_id as cohort_definition_id, PC.total as person_count, coalesce(FC.total, 0) as final_count, 1 as mode_id
FROM
(select count_big(event_id) as total from #best_events) PC,
(select sum(sr.person_count) as total
  from @results_database_schema.cohort_inclusion_result sr
  CROSS JOIN (select count(*) as total_rules from #inclusion_rules) RuleTotal
  where sr.mode_id = 1 and sr.cohort_definition_id = @target_cohort_id and sr.inclusion_rule_mask = POWER(cast(2 as bigint),RuleTotal.total_rules)-1
) FC
;

-- END: Inclusion Impact Analysis - person

TRUNCATE TABLE #best_events;
DROP TABLE #best_events;

TRUNCATE TABLE #inclusion_rules;
DROP TABLE #inclusion_rules;
}



TRUNCATE TABLE #cohort_rows;
DROP TABLE #cohort_rows;

TRUNCATE TABLE #final_cohort;
DROP TABLE #final_cohort;

TRUNCATE TABLE #inclusion_events;
DROP TABLE #inclusion_events;

TRUNCATE TABLE #qualified_events;
DROP TABLE #qualified_events;

TRUNCATE TABLE #included_events;
DROP TABLE #included_events;

TRUNCATE TABLE #Codesets;
DROP TABLE #Codesets;
