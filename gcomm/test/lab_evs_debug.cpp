/*
 * Copyright (C) 2009-2020 Codership Oy <info@codership.com>
 */

/*!
 * @file Unit tests for refactored EVS
 */


/*
 *
 *
 *2024-01-04T16:45:16.630357-05:00 0 [Warning] [MY-000000] [Galera] evs::proto(48f31786-8084, GATHER, view_id(REG,48f31786-8084,62)) install timer expired
evs::proto(evs::proto(48f31786-8084, GATHER, view_id(REG,48f31786-8084,62)), GATHER) {
current_view=Current view of cluster as seen by this node
view (view_id(REG,48f31786-8084,62)
memb {
        48f31786-8084,0
        53eff919-a187,0
        660e2c3d-b169,0
        79c10d9a-843e,0
        }
joined {
        }
left {
        }
partitioned {
        }
),

 * input_map=evs::input_map: {aru_seq=94928367,safe_seq=94928365,node_index=node: {idx=0,range=[94928375,94928374],safe_seq=94928367} node: {idx=1,range=[94928375,94928374],safe_seq=94928367} node: {idx=2,range=[94928375,94928374],safe_seq=94928367} node: {idx=3,range=[94928368,94928367],safe_seq=94928365} },
fifo_seq=698909113,
last_sent=94928374,
known:
48f31786-8084 at 
{o=1,s=0,i=0,fs=-1,jm=
{v=1,t=4,ut=255,o=1,s=94928365,sr=-1,as=94928367,f=0,src=48f31786-8084,srcvid=view_id(REG,48f31786-8084,62),insvid=view_id(UNKNOWN,00000000-0000,0),ru=00000000-0000,r=[-1,-1],fs=698909113,nl=(
        48f31786-8084, {o=1,s=0,e=0,ls=-1,vid=view_id(REG,48f31786-8084,62),ss=94928367,ir=[94928375,94928374],}
        53eff919-a187, {o=1,s=0,e=0,ls=-1,vid=view_id(REG,48f31786-8084,62),ss=94928367,ir=[94928375,94928374],}
        660e2c3d-b169, {o=1,s=0,e=0,ls=-1,vid=view_id(REG,48f31786-8084,62),ss=94928367,ir=[94928375,94928374],}
        79c10d9a-843e, {o=0,s=1,e=0,ls=94928367,vid=view_id(REG,48f31786-8084,62),ss=94928365,ir=[94928368,94928367],}
)
},
}
53eff919-a187 at ssl://ip:port
{o=1,s=0,i=0,fs=414331849,jm=
{v=1,t=4,ut=255,o=1,s=94928365,sr=-1,as=94928367,f=4,src=53eff919-a187,srcvid=view_id(REG,48f31786-8084,62),insvid=view_id(UNKNOWN,00000000-0000,0),ru=00000000-0000,r=[-1,-1],fs=414331849,nl=(
        48f31786-8084, {o=1,s=0,e=0,ls=-1,vid=view_id(REG,48f31786-8084,62),ss=94928367,ir=[94928375,94928374],}
        53eff919-a187, {o=1,s=0,e=0,ls=-1,vid=view_id(REG,48f31786-8084,62),ss=94928367,ir=[94928375,94928374],}
        660e2c3d-b169, {o=1,s=0,e=0,ls=-1,vid=view_id(REG,48f31786-8084,62),ss=94928367,ir=[94928375,94928374],}
        79c10d9a-843e, {o=0,s=1,e=0,ls=94928367,vid=view_id(REG,48f31786-8084,62),ss=94928365,ir=[94928368,94928367],}
)
},
}
660e2c3d-b169 at ssl://ip:port
{o=1,s=0,i=0,fs=188631162,jm=
{v=1,t=4,ut=255,o=1,s=94928365,sr=-1,as=94928367,f=4,src=660e2c3d-b169,srcvid=view_id(REG,48f31786-8084,62),insvid=view_id(UNKNOWN,00000000-0000,0),ru=00000000-0000,r=[-1,-1],fs=188631162,nl=(
        48f31786-8084, {o=1,s=0,e=0,ls=-1,vid=view_id(REG,48f31786-8084,62),ss=94928367,ir=[94928375,94928374],}
        53eff919-a187, {o=1,s=0,e=0,ls=-1,vid=view_id(REG,48f31786-8084,62),ss=94928367,ir=[94928375,94928374],}
        660e2c3d-b169, {o=1,s=0,e=0,ls=-1,vid=view_id(REG,48f31786-8084,62),ss=94928367,ir=[94928375,94928374],}
        79c10d9a-843e, {o=0,s=1,e=0,ls=94928367,vid=view_id(REG,48f31786-8084,62),ss=94928365,ir=[94928368,94928367],}
)
},
}
79c10d9a-843e at ssl://ip:port
{o=0,s=1,i=0,fs=382623798,lm=
{v=1,t=6,ut=255,o=1,s=94928367,sr=-1,as=94928365,f=4,src=79c10d9a-843e,srcvid=view_id(REG,48f31786-8084,62),insvid=view_id(UNKNOWN,00000000-0000,0),ru=00000000-0000,r=[-1,-1],fs=382623798,nl=(
) 
 *
 */

#include "evs_proto.hpp"
#include "evs_input_map2.hpp"
#include "evs_message2.hpp"
#include "evs_seqno.hpp"

#include "check_gcomm.hpp"
#include "check_trace.hpp"

#include "gcomm/conf.hpp"

#include "gu_asio.hpp" // gu::ssl_register_params()

#include <stdexcept>
#include <vector>
#include <set>
#include <iostream>

#include "check.h"

//
// set GALERA_TEST_DETERMINISTIC env
// variable before running pc test suite.
//

using namespace std;
using namespace std::rel_ops;
using namespace gu::datetime;
using namespace gcomm;
using namespace gcomm::evs;
using gu::DeleteObject;

/*
struct OneNodeFixture
{
    struct Configs
    {
        Configs()
            : conf1()
        {
            gu::ssl_register_params(conf1);
            gcomm::Conf::register_params(conf1);
        }
        gu::Config conf1;  // Config for node1
    };
    OneNodeFixture()
        : conf()
        , UUID(1)(1)
        , tr1(UUID(1))
        , evs1(conf.conf1, UUID(1), 0)
        //, top1(conf.conf1)
    {
    }
    Configs conf;
    const gcomm::UUID UUID(1); // UUID of node1
    DummyTransport tr1; // Transport for node1
    gcomm::evs::Proto evs1; // Proto for node1
    //DummyUser top1;      // Top level layer for node1
};
*/

struct InitGuConf
{
    explicit InitGuConf(gu::Config& conf) { gcomm::Conf::register_params(conf); }
};
static gu::Config&
static_gu_conf()
{
    static gu::Config conf;
    static InitGuConf init(conf);

    return conf;
}
START_TEST(lab)
{
	std::cout << "START\n";

    ViewId view_id(V_TRANS, UUID(1), 4567);
    seqno_t seq(94928365), aru_seq(94928367), seq_range(-1);

    UserMessage um(1, UUID(1), view_id, seq, aru_seq, seq_range, O_UNRELIABLE, 698909113, 0xab,
                   Message::F_SOURCE);

    MessageNodeList node_list;
    node_list.insert(make_pair(UUID(1), MessageNode(true, false, 0, false, -1,
                                                    ViewId(V_REG), 94928367,
                                                    Range(94928375,94928374))));
    node_list.insert(make_pair(UUID(2), MessageNode(true, false, 0, false, -1,
                                                    ViewId(V_REG), 94928367,
                                                    Range(94928375,94928374))));
    node_list.insert(make_pair(UUID(3), MessageNode(true, false, 0, false, -1,
                                                    ViewId(V_REG), 94928367,
                                                    Range(94928375,94928374))));
    node_list.insert(make_pair(UUID(4), MessageNode(false, true, 0, false, 94928367,
                                                    ViewId(V_REG), 94928365,
                                                    Range(94928368,94928367))));
    
	JoinMessage jm1(1, UUID(1), view_id, 94928367, 94928367, 698909113, node_list);
    jm1.set_source(UUID(1));

	JoinMessage jm2(1, UUID(2), view_id, 94928367, 94928367, 414331849, node_list);
    //jm2.set_source(UUID(2));
	
	JoinMessage jm3(1, UUID(3), view_id, 94928367, 94928367, 188631162, node_list);
	
	LeaveMessage lm1(1, UUID(4), view_id, 94928367, 94928365, 382623798);
	
	
	View view(V_NONE, view_id);




    gu::Config& gu_conf(static_gu_conf());
    gcomm::Conf::register_params(gu_conf);
    const string conf = "evs://";
    //list<Protolay*> protos;

    //protos.push_back(new evs::Proto(gu_conf, uuid, 0, conf));

	//gu::Config conf;
	//gu::URI uri = gu::URI("evs://");
    //gcomm::evs::Proto evs1(conf, UUID(1), 0, uri , 0, &view); 
    gcomm::evs::Proto evs1(gu_conf, UUID(1), 0, conf, std::numeric_limits<size_t>::max(), &view); 
	//evs1.shift_to(gcomm::evs::Proto::S_GATHER, false);

	
	gcomm::evs::Node node1(evs1);
	//node1.set_join_message(&jm);
	evs1.set_join(jm1, UUID(1));
	evs1.set_join(jm2, UUID(2));
	evs1.set_join(jm3, UUID(3));
	evs1.set_leave(lm1, UUID(4));
	
	std::cout << "dump" << std::endl;	
	std::cout << evs1 << std::endl;	

	//f.evs1.handle_install_timer();
	//gcomm::evs::Proto::handle_install_timer()

	exit(1);

}
END_TEST


Suite* lab_suite()
{
	  std::cout << "creating suite\n";
      Suite* s = suite_create("gcomm::evs");
      TCase* tc;

	  std::cout << "creating case\n";
      tc = tcase_create("lab");
      tcase_add_test(tc, lab);
      suite_add_tcase(s, tc);
	  return s;
}

