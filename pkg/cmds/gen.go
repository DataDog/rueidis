package cmds

import "strconv"

type aclCat struct {
	cs []string
}

func (c aclCat) Categoryname(categoryname string) aclCatCategoryname {
	return aclCatCategoryname{cs: append(c.cs, categoryname)}
}

func (c aclCat) Build() []string {
	return c.cs
}

func (b *Builder) AclCat() (c aclCat) {
	c.cs = append(b.get(), "ACL", "CAT")
	return
}

type aclCatCategoryname struct {
	cs []string
}

func (c aclCatCategoryname) Build() []string {
	return c.cs
}

type aclDeluser struct {
	cs []string
}

func (c aclDeluser) Username(username ...string) aclDeluserUsername {
	return aclDeluserUsername{cs: append(c.cs, username...)}
}

func (b *Builder) AclDeluser() (c aclDeluser) {
	c.cs = append(b.get(), "ACL", "DELUSER")
	return
}

type aclDeluserUsername struct {
	cs []string
}

func (c aclDeluserUsername) Username(username ...string) aclDeluserUsername {
	return aclDeluserUsername{cs: append(c.cs, username...)}
}

func (c aclDeluserUsername) Build() []string {
	return c.cs
}

type aclGenpass struct {
	cs []string
}

func (c aclGenpass) Bits(bits int64) aclGenpassBits {
	return aclGenpassBits{cs: append(c.cs, strconv.FormatInt(bits, 10))}
}

func (c aclGenpass) Build() []string {
	return c.cs
}

func (b *Builder) AclGenpass() (c aclGenpass) {
	c.cs = append(b.get(), "ACL", "GENPASS")
	return
}

type aclGenpassBits struct {
	cs []string
}

func (c aclGenpassBits) Build() []string {
	return c.cs
}

type aclGetuser struct {
	cs []string
}

func (c aclGetuser) Username(username string) aclGetuserUsername {
	return aclGetuserUsername{cs: append(c.cs, username)}
}

func (b *Builder) AclGetuser() (c aclGetuser) {
	c.cs = append(b.get(), "ACL", "GETUSER")
	return
}

type aclGetuserUsername struct {
	cs []string
}

func (c aclGetuserUsername) Build() []string {
	return c.cs
}

type aclHelp struct {
	cs []string
}

func (c aclHelp) Build() []string {
	return c.cs
}

func (b *Builder) AclHelp() (c aclHelp) {
	c.cs = append(b.get(), "ACL", "HELP")
	return
}

type aclList struct {
	cs []string
}

func (c aclList) Build() []string {
	return c.cs
}

func (b *Builder) AclList() (c aclList) {
	c.cs = append(b.get(), "ACL", "LIST")
	return
}

type aclLoad struct {
	cs []string
}

func (c aclLoad) Build() []string {
	return c.cs
}

func (b *Builder) AclLoad() (c aclLoad) {
	c.cs = append(b.get(), "ACL", "LOAD")
	return
}

type aclLog struct {
	cs []string
}

func (c aclLog) CountOrReset(countOrReset string) aclLogCountOrReset {
	return aclLogCountOrReset{cs: append(c.cs, countOrReset)}
}

func (c aclLog) Build() []string {
	return c.cs
}

func (b *Builder) AclLog() (c aclLog) {
	c.cs = append(b.get(), "ACL", "LOG")
	return
}

type aclLogCountOrReset struct {
	cs []string
}

func (c aclLogCountOrReset) Build() []string {
	return c.cs
}

type aclSave struct {
	cs []string
}

func (c aclSave) Build() []string {
	return c.cs
}

func (b *Builder) AclSave() (c aclSave) {
	c.cs = append(b.get(), "ACL", "SAVE")
	return
}

type aclSetuser struct {
	cs []string
}

func (c aclSetuser) Username(username string) aclSetuserUsername {
	return aclSetuserUsername{cs: append(c.cs, username)}
}

func (b *Builder) AclSetuser() (c aclSetuser) {
	c.cs = append(b.get(), "ACL", "SETUSER")
	return
}

type aclSetuserRule struct {
	cs []string
}

func (c aclSetuserRule) Rule(rule ...string) aclSetuserRule {
	return aclSetuserRule{cs: append(c.cs, rule...)}
}

func (c aclSetuserRule) Build() []string {
	return c.cs
}

type aclSetuserUsername struct {
	cs []string
}

func (c aclSetuserUsername) Rule(rule ...string) aclSetuserRule {
	return aclSetuserRule{cs: append(c.cs, rule...)}
}

func (c aclSetuserUsername) Build() []string {
	return c.cs
}

type aclUsers struct {
	cs []string
}

func (c aclUsers) Build() []string {
	return c.cs
}

func (b *Builder) AclUsers() (c aclUsers) {
	c.cs = append(b.get(), "ACL", "USERS")
	return
}

type aclWhoami struct {
	cs []string
}

func (c aclWhoami) Build() []string {
	return c.cs
}

func (b *Builder) AclWhoami() (c aclWhoami) {
	c.cs = append(b.get(), "ACL", "WHOAMI")
	return
}

type rAppend struct {
	cs []string
}

func (c rAppend) Key(key string) appendKey {
	return appendKey{cs: append(c.cs, key)}
}

func (b *Builder) Append() (c rAppend) {
	c.cs = append(b.get(), "APPEND")
	return
}

type appendKey struct {
	cs []string
}

func (c appendKey) Value(value string) appendValue {
	return appendValue{cs: append(c.cs, value)}
}

type appendValue struct {
	cs []string
}

func (c appendValue) Build() []string {
	return c.cs
}

type asking struct {
	cs []string
}

func (c asking) Build() []string {
	return c.cs
}

func (b *Builder) Asking() (c asking) {
	c.cs = append(b.get(), "ASKING")
	return
}

type auth struct {
	cs []string
}

func (c auth) Username(username string) authUsername {
	return authUsername{cs: append(c.cs, username)}
}

func (c auth) Password(password string) authPassword {
	return authPassword{cs: append(c.cs, password)}
}

func (b *Builder) Auth() (c auth) {
	c.cs = append(b.get(), "AUTH")
	return
}

type authPassword struct {
	cs []string
}

func (c authPassword) Build() []string {
	return c.cs
}

type authUsername struct {
	cs []string
}

func (c authUsername) Password(password string) authPassword {
	return authPassword{cs: append(c.cs, password)}
}

type bgrewriteaof struct {
	cs []string
}

func (c bgrewriteaof) Build() []string {
	return c.cs
}

func (b *Builder) Bgrewriteaof() (c bgrewriteaof) {
	c.cs = append(b.get(), "BGREWRITEAOF")
	return
}

type bgsave struct {
	cs []string
}

func (c bgsave) Schedule() bgsaveScheduleSchedule {
	return bgsaveScheduleSchedule{cs: append(c.cs, "SCHEDULE")}
}

func (c bgsave) Build() []string {
	return c.cs
}

func (b *Builder) Bgsave() (c bgsave) {
	c.cs = append(b.get(), "BGSAVE")
	return
}

type bgsaveScheduleSchedule struct {
	cs []string
}

func (c bgsaveScheduleSchedule) Build() []string {
	return c.cs
}

type bitcount struct {
	cs []string
}

func (c bitcount) Key(key string) bitcountKey {
	return bitcountKey{cs: append(c.cs, key)}
}

func (b *Builder) Bitcount() (c bitcount) {
	c.cs = append(b.get(), "BITCOUNT")
	return
}

type bitcountKey struct {
	cs []string
}

func (c bitcountKey) StartEnd(start int64, end int64) bitcountStartEnd {
	return bitcountStartEnd{cs: append(c.cs, strconv.FormatInt(start, 10), strconv.FormatInt(end, 10))}
}

func (c bitcountKey) Build() []string {
	return c.cs
}

type bitcountStartEnd struct {
	cs []string
}

func (c bitcountStartEnd) Build() []string {
	return c.cs
}

type bitfield struct {
	cs []string
}

func (c bitfield) Key(key string) bitfieldKey {
	return bitfieldKey{cs: append(c.cs, key)}
}

func (b *Builder) Bitfield() (c bitfield) {
	c.cs = append(b.get(), "BITFIELD")
	return
}

type bitfieldFail struct {
	cs []string
}

func (c bitfieldFail) Build() []string {
	return c.cs
}

type bitfieldGet struct {
	cs []string
}

func (c bitfieldGet) Set(typ string, offset int64, value int64) bitfieldSet {
	return bitfieldSet{cs: append(c.cs, "SET", typ, strconv.FormatInt(offset, 10), strconv.FormatInt(value, 10))}
}

func (c bitfieldGet) Incrby(typ string, offset int64, increment int64) bitfieldIncrby {
	return bitfieldIncrby{cs: append(c.cs, "INCRBY", typ, strconv.FormatInt(offset, 10), strconv.FormatInt(increment, 10))}
}

func (c bitfieldGet) Wrap() bitfieldWrap {
	return bitfieldWrap{cs: append(c.cs, "WRAP")}
}

func (c bitfieldGet) Sat() bitfieldSat {
	return bitfieldSat{cs: append(c.cs, "SAT")}
}

func (c bitfieldGet) Fail() bitfieldFail {
	return bitfieldFail{cs: append(c.cs, "FAIL")}
}

func (c bitfieldGet) Build() []string {
	return c.cs
}

type bitfieldIncrby struct {
	cs []string
}

func (c bitfieldIncrby) Wrap() bitfieldWrap {
	return bitfieldWrap{cs: append(c.cs, "WRAP")}
}

func (c bitfieldIncrby) Sat() bitfieldSat {
	return bitfieldSat{cs: append(c.cs, "SAT")}
}

func (c bitfieldIncrby) Fail() bitfieldFail {
	return bitfieldFail{cs: append(c.cs, "FAIL")}
}

func (c bitfieldIncrby) Build() []string {
	return c.cs
}

type bitfieldKey struct {
	cs []string
}

func (c bitfieldKey) Get(typ string, offset int64) bitfieldGet {
	return bitfieldGet{cs: append(c.cs, "GET", typ, strconv.FormatInt(offset, 10))}
}

func (c bitfieldKey) Set(typ string, offset int64, value int64) bitfieldSet {
	return bitfieldSet{cs: append(c.cs, "SET", typ, strconv.FormatInt(offset, 10), strconv.FormatInt(value, 10))}
}

func (c bitfieldKey) Incrby(typ string, offset int64, increment int64) bitfieldIncrby {
	return bitfieldIncrby{cs: append(c.cs, "INCRBY", typ, strconv.FormatInt(offset, 10), strconv.FormatInt(increment, 10))}
}

func (c bitfieldKey) Wrap() bitfieldWrap {
	return bitfieldWrap{cs: append(c.cs, "WRAP")}
}

func (c bitfieldKey) Sat() bitfieldSat {
	return bitfieldSat{cs: append(c.cs, "SAT")}
}

func (c bitfieldKey) Fail() bitfieldFail {
	return bitfieldFail{cs: append(c.cs, "FAIL")}
}

func (c bitfieldKey) Build() []string {
	return c.cs
}

type bitfieldRo struct {
	cs []string
}

func (c bitfieldRo) Key(key string) bitfieldRoKey {
	return bitfieldRoKey{cs: append(c.cs, key)}
}

func (b *Builder) BitfieldRo() (c bitfieldRo) {
	c.cs = append(b.get(), "BITFIELD_RO")
	return
}

type bitfieldRoGet struct {
	cs []string
}

func (c bitfieldRoGet) Build() []string {
	return c.cs
}

type bitfieldRoKey struct {
	cs []string
}

func (c bitfieldRoKey) Get(typ string, offset int64) bitfieldRoGet {
	return bitfieldRoGet{cs: append(c.cs, "GET", typ, strconv.FormatInt(offset, 10))}
}

type bitfieldSat struct {
	cs []string
}

func (c bitfieldSat) Build() []string {
	return c.cs
}

type bitfieldSet struct {
	cs []string
}

func (c bitfieldSet) Incrby(typ string, offset int64, increment int64) bitfieldIncrby {
	return bitfieldIncrby{cs: append(c.cs, "INCRBY", typ, strconv.FormatInt(offset, 10), strconv.FormatInt(increment, 10))}
}

func (c bitfieldSet) Wrap() bitfieldWrap {
	return bitfieldWrap{cs: append(c.cs, "WRAP")}
}

func (c bitfieldSet) Sat() bitfieldSat {
	return bitfieldSat{cs: append(c.cs, "SAT")}
}

func (c bitfieldSet) Fail() bitfieldFail {
	return bitfieldFail{cs: append(c.cs, "FAIL")}
}

func (c bitfieldSet) Build() []string {
	return c.cs
}

type bitfieldWrap struct {
	cs []string
}

func (c bitfieldWrap) Build() []string {
	return c.cs
}

type bitop struct {
	cs []string
}

func (c bitop) Operation(operation string) bitopOperation {
	return bitopOperation{cs: append(c.cs, operation)}
}

func (b *Builder) Bitop() (c bitop) {
	c.cs = append(b.get(), "BITOP")
	return
}

type bitopDestkey struct {
	cs []string
}

func (c bitopDestkey) Key(key ...string) bitopKey {
	return bitopKey{cs: append(c.cs, key...)}
}

type bitopKey struct {
	cs []string
}

func (c bitopKey) Key(key ...string) bitopKey {
	return bitopKey{cs: append(c.cs, key...)}
}

func (c bitopKey) Build() []string {
	return c.cs
}

type bitopOperation struct {
	cs []string
}

func (c bitopOperation) Destkey(destkey string) bitopDestkey {
	return bitopDestkey{cs: append(c.cs, destkey)}
}

type bitpos struct {
	cs []string
}

func (c bitpos) Key(key string) bitposKey {
	return bitposKey{cs: append(c.cs, key)}
}

func (b *Builder) Bitpos() (c bitpos) {
	c.cs = append(b.get(), "BITPOS")
	return
}

type bitposBit struct {
	cs []string
}

func (c bitposBit) Start(start int64) bitposIndexStart {
	return bitposIndexStart{cs: append(c.cs, strconv.FormatInt(start, 10))}
}

type bitposIndexEnd struct {
	cs []string
}

func (c bitposIndexEnd) Build() []string {
	return c.cs
}

type bitposIndexStart struct {
	cs []string
}

func (c bitposIndexStart) End(end int64) bitposIndexEnd {
	return bitposIndexEnd{cs: append(c.cs, strconv.FormatInt(end, 10))}
}

func (c bitposIndexStart) Build() []string {
	return c.cs
}

type bitposKey struct {
	cs []string
}

func (c bitposKey) Bit(bit int64) bitposBit {
	return bitposBit{cs: append(c.cs, strconv.FormatInt(bit, 10))}
}

type blmove struct {
	cs []string
}

func (c blmove) Source(source string) blmoveSource {
	return blmoveSource{cs: append(c.cs, source)}
}

func (b *Builder) Blmove() (c blmove) {
	c.cs = append(b.get(), "BLMOVE")
	return
}

type blmoveDestination struct {
	cs []string
}

func (c blmoveDestination) Left() blmoveWherefromLeft {
	return blmoveWherefromLeft{cs: append(c.cs, "LEFT")}
}

func (c blmoveDestination) Right() blmoveWherefromRight {
	return blmoveWherefromRight{cs: append(c.cs, "RIGHT")}
}

type blmoveSource struct {
	cs []string
}

func (c blmoveSource) Destination(destination string) blmoveDestination {
	return blmoveDestination{cs: append(c.cs, destination)}
}

type blmoveTimeout struct {
	cs []string
}

func (c blmoveTimeout) Build() []string {
	return c.cs
}

type blmoveWherefromLeft struct {
	cs []string
}

func (c blmoveWherefromLeft) Left() blmoveWheretoLeft {
	return blmoveWheretoLeft{cs: append(c.cs, "LEFT")}
}

func (c blmoveWherefromLeft) Right() blmoveWheretoRight {
	return blmoveWheretoRight{cs: append(c.cs, "RIGHT")}
}

type blmoveWherefromRight struct {
	cs []string
}

func (c blmoveWherefromRight) Left() blmoveWheretoLeft {
	return blmoveWheretoLeft{cs: append(c.cs, "LEFT")}
}

func (c blmoveWherefromRight) Right() blmoveWheretoRight {
	return blmoveWheretoRight{cs: append(c.cs, "RIGHT")}
}

type blmoveWheretoLeft struct {
	cs []string
}

func (c blmoveWheretoLeft) Timeout(timeout float64) blmoveTimeout {
	return blmoveTimeout{cs: append(c.cs, strconv.FormatFloat(timeout, 'f', -1, 64))}
}

type blmoveWheretoRight struct {
	cs []string
}

func (c blmoveWheretoRight) Timeout(timeout float64) blmoveTimeout {
	return blmoveTimeout{cs: append(c.cs, strconv.FormatFloat(timeout, 'f', -1, 64))}
}

type blmpop struct {
	cs []string
}

func (c blmpop) Timeout(timeout float64) blmpopTimeout {
	return blmpopTimeout{cs: append(c.cs, strconv.FormatFloat(timeout, 'f', -1, 64))}
}

func (b *Builder) Blmpop() (c blmpop) {
	c.cs = append(b.get(), "BLMPOP")
	return
}

type blmpopCount struct {
	cs []string
}

func (c blmpopCount) Build() []string {
	return c.cs
}

type blmpopKey struct {
	cs []string
}

func (c blmpopKey) Left() blmpopWhereLeft {
	return blmpopWhereLeft{cs: append(c.cs, "LEFT")}
}

func (c blmpopKey) Right() blmpopWhereRight {
	return blmpopWhereRight{cs: append(c.cs, "RIGHT")}
}

func (c blmpopKey) Key(key ...string) blmpopKey {
	return blmpopKey{cs: append(c.cs, key...)}
}

type blmpopNumkeys struct {
	cs []string
}

func (c blmpopNumkeys) Key(key ...string) blmpopKey {
	return blmpopKey{cs: append(c.cs, key...)}
}

func (c blmpopNumkeys) Left() blmpopWhereLeft {
	return blmpopWhereLeft{cs: append(c.cs, "LEFT")}
}

func (c blmpopNumkeys) Right() blmpopWhereRight {
	return blmpopWhereRight{cs: append(c.cs, "RIGHT")}
}

type blmpopTimeout struct {
	cs []string
}

func (c blmpopTimeout) Numkeys(numkeys int64) blmpopNumkeys {
	return blmpopNumkeys{cs: append(c.cs, strconv.FormatInt(numkeys, 10))}
}

type blmpopWhereLeft struct {
	cs []string
}

func (c blmpopWhereLeft) Count(count int64) blmpopCount {
	return blmpopCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c blmpopWhereLeft) Build() []string {
	return c.cs
}

type blmpopWhereRight struct {
	cs []string
}

func (c blmpopWhereRight) Count(count int64) blmpopCount {
	return blmpopCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c blmpopWhereRight) Build() []string {
	return c.cs
}

type blpop struct {
	cs []string
}

func (c blpop) Key(key ...string) blpopKey {
	return blpopKey{cs: append(c.cs, key...)}
}

func (b *Builder) Blpop() (c blpop) {
	c.cs = append(b.get(), "BLPOP")
	return
}

type blpopKey struct {
	cs []string
}

func (c blpopKey) Timeout(timeout float64) blpopTimeout {
	return blpopTimeout{cs: append(c.cs, strconv.FormatFloat(timeout, 'f', -1, 64))}
}

func (c blpopKey) Key(key ...string) blpopKey {
	return blpopKey{cs: append(c.cs, key...)}
}

type blpopTimeout struct {
	cs []string
}

func (c blpopTimeout) Build() []string {
	return c.cs
}

type brpop struct {
	cs []string
}

func (c brpop) Key(key ...string) brpopKey {
	return brpopKey{cs: append(c.cs, key...)}
}

func (b *Builder) Brpop() (c brpop) {
	c.cs = append(b.get(), "BRPOP")
	return
}

type brpopKey struct {
	cs []string
}

func (c brpopKey) Timeout(timeout float64) brpopTimeout {
	return brpopTimeout{cs: append(c.cs, strconv.FormatFloat(timeout, 'f', -1, 64))}
}

func (c brpopKey) Key(key ...string) brpopKey {
	return brpopKey{cs: append(c.cs, key...)}
}

type brpopTimeout struct {
	cs []string
}

func (c brpopTimeout) Build() []string {
	return c.cs
}

type brpoplpush struct {
	cs []string
}

func (c brpoplpush) Source(source string) brpoplpushSource {
	return brpoplpushSource{cs: append(c.cs, source)}
}

func (b *Builder) Brpoplpush() (c brpoplpush) {
	c.cs = append(b.get(), "BRPOPLPUSH")
	return
}

type brpoplpushDestination struct {
	cs []string
}

func (c brpoplpushDestination) Timeout(timeout float64) brpoplpushTimeout {
	return brpoplpushTimeout{cs: append(c.cs, strconv.FormatFloat(timeout, 'f', -1, 64))}
}

type brpoplpushSource struct {
	cs []string
}

func (c brpoplpushSource) Destination(destination string) brpoplpushDestination {
	return brpoplpushDestination{cs: append(c.cs, destination)}
}

type brpoplpushTimeout struct {
	cs []string
}

func (c brpoplpushTimeout) Build() []string {
	return c.cs
}

type bzpopmax struct {
	cs []string
}

func (c bzpopmax) Key(key ...string) bzpopmaxKey {
	return bzpopmaxKey{cs: append(c.cs, key...)}
}

func (b *Builder) Bzpopmax() (c bzpopmax) {
	c.cs = append(b.get(), "BZPOPMAX")
	return
}

type bzpopmaxKey struct {
	cs []string
}

func (c bzpopmaxKey) Timeout(timeout float64) bzpopmaxTimeout {
	return bzpopmaxTimeout{cs: append(c.cs, strconv.FormatFloat(timeout, 'f', -1, 64))}
}

func (c bzpopmaxKey) Key(key ...string) bzpopmaxKey {
	return bzpopmaxKey{cs: append(c.cs, key...)}
}

type bzpopmaxTimeout struct {
	cs []string
}

func (c bzpopmaxTimeout) Build() []string {
	return c.cs
}

type bzpopmin struct {
	cs []string
}

func (c bzpopmin) Key(key ...string) bzpopminKey {
	return bzpopminKey{cs: append(c.cs, key...)}
}

func (b *Builder) Bzpopmin() (c bzpopmin) {
	c.cs = append(b.get(), "BZPOPMIN")
	return
}

type bzpopminKey struct {
	cs []string
}

func (c bzpopminKey) Timeout(timeout float64) bzpopminTimeout {
	return bzpopminTimeout{cs: append(c.cs, strconv.FormatFloat(timeout, 'f', -1, 64))}
}

func (c bzpopminKey) Key(key ...string) bzpopminKey {
	return bzpopminKey{cs: append(c.cs, key...)}
}

type bzpopminTimeout struct {
	cs []string
}

func (c bzpopminTimeout) Build() []string {
	return c.cs
}

type clientCaching struct {
	cs []string
}

func (c clientCaching) Yes() clientCachingModeYes {
	return clientCachingModeYes{cs: append(c.cs, "YES")}
}

func (c clientCaching) No() clientCachingModeNo {
	return clientCachingModeNo{cs: append(c.cs, "NO")}
}

func (b *Builder) ClientCaching() (c clientCaching) {
	c.cs = append(b.get(), "CLIENT", "CACHING")
	return
}

type clientCachingModeNo struct {
	cs []string
}

func (c clientCachingModeNo) Build() []string {
	return c.cs
}

type clientCachingModeYes struct {
	cs []string
}

func (c clientCachingModeYes) Build() []string {
	return c.cs
}

type clientGetname struct {
	cs []string
}

func (c clientGetname) Build() []string {
	return c.cs
}

func (b *Builder) ClientGetname() (c clientGetname) {
	c.cs = append(b.get(), "CLIENT", "GETNAME")
	return
}

type clientGetredir struct {
	cs []string
}

func (c clientGetredir) Build() []string {
	return c.cs
}

func (b *Builder) ClientGetredir() (c clientGetredir) {
	c.cs = append(b.get(), "CLIENT", "GETREDIR")
	return
}

type clientId struct {
	cs []string
}

func (c clientId) Build() []string {
	return c.cs
}

func (b *Builder) ClientId() (c clientId) {
	c.cs = append(b.get(), "CLIENT", "ID")
	return
}

type clientInfo struct {
	cs []string
}

func (c clientInfo) Build() []string {
	return c.cs
}

func (b *Builder) ClientInfo() (c clientInfo) {
	c.cs = append(b.get(), "CLIENT", "INFO")
	return
}

type clientKill struct {
	cs []string
}

func (c clientKill) IpPort(ipPort string) clientKillIpPort {
	return clientKillIpPort{cs: append(c.cs, ipPort)}
}

func (c clientKill) Id(clientId int64) clientKillId {
	return clientKillId{cs: append(c.cs, "ID", strconv.FormatInt(clientId, 10))}
}

func (c clientKill) Normal() clientKillNormal {
	return clientKillNormal{cs: append(c.cs, "normal")}
}

func (c clientKill) Master() clientKillMaster {
	return clientKillMaster{cs: append(c.cs, "master")}
}

func (c clientKill) Slave() clientKillSlave {
	return clientKillSlave{cs: append(c.cs, "slave")}
}

func (c clientKill) Pubsub() clientKillPubsub {
	return clientKillPubsub{cs: append(c.cs, "pubsub")}
}

func (c clientKill) User(username string) clientKillUser {
	return clientKillUser{cs: append(c.cs, "USER", username)}
}

func (c clientKill) Addr(ipPort string) clientKillAddr {
	return clientKillAddr{cs: append(c.cs, "ADDR", ipPort)}
}

func (c clientKill) Laddr(ipPort string) clientKillLaddr {
	return clientKillLaddr{cs: append(c.cs, "LADDR", ipPort)}
}

func (c clientKill) Skipme(yesNo string) clientKillSkipme {
	return clientKillSkipme{cs: append(c.cs, "SKIPME", yesNo)}
}

func (c clientKill) Build() []string {
	return c.cs
}

func (b *Builder) ClientKill() (c clientKill) {
	c.cs = append(b.get(), "CLIENT", "KILL")
	return
}

type clientKillAddr struct {
	cs []string
}

func (c clientKillAddr) Laddr(ipPort string) clientKillLaddr {
	return clientKillLaddr{cs: append(c.cs, "LADDR", ipPort)}
}

func (c clientKillAddr) Skipme(yesNo string) clientKillSkipme {
	return clientKillSkipme{cs: append(c.cs, "SKIPME", yesNo)}
}

func (c clientKillAddr) Build() []string {
	return c.cs
}

type clientKillId struct {
	cs []string
}

func (c clientKillId) Normal() clientKillNormal {
	return clientKillNormal{cs: append(c.cs, "normal")}
}

func (c clientKillId) Master() clientKillMaster {
	return clientKillMaster{cs: append(c.cs, "master")}
}

func (c clientKillId) Slave() clientKillSlave {
	return clientKillSlave{cs: append(c.cs, "slave")}
}

func (c clientKillId) Pubsub() clientKillPubsub {
	return clientKillPubsub{cs: append(c.cs, "pubsub")}
}

func (c clientKillId) User(username string) clientKillUser {
	return clientKillUser{cs: append(c.cs, "USER", username)}
}

func (c clientKillId) Addr(ipPort string) clientKillAddr {
	return clientKillAddr{cs: append(c.cs, "ADDR", ipPort)}
}

func (c clientKillId) Laddr(ipPort string) clientKillLaddr {
	return clientKillLaddr{cs: append(c.cs, "LADDR", ipPort)}
}

func (c clientKillId) Skipme(yesNo string) clientKillSkipme {
	return clientKillSkipme{cs: append(c.cs, "SKIPME", yesNo)}
}

func (c clientKillId) Build() []string {
	return c.cs
}

type clientKillIpPort struct {
	cs []string
}

func (c clientKillIpPort) Id(clientId int64) clientKillId {
	return clientKillId{cs: append(c.cs, "ID", strconv.FormatInt(clientId, 10))}
}

func (c clientKillIpPort) Normal() clientKillNormal {
	return clientKillNormal{cs: append(c.cs, "normal")}
}

func (c clientKillIpPort) Master() clientKillMaster {
	return clientKillMaster{cs: append(c.cs, "master")}
}

func (c clientKillIpPort) Slave() clientKillSlave {
	return clientKillSlave{cs: append(c.cs, "slave")}
}

func (c clientKillIpPort) Pubsub() clientKillPubsub {
	return clientKillPubsub{cs: append(c.cs, "pubsub")}
}

func (c clientKillIpPort) User(username string) clientKillUser {
	return clientKillUser{cs: append(c.cs, "USER", username)}
}

func (c clientKillIpPort) Addr(ipPort string) clientKillAddr {
	return clientKillAddr{cs: append(c.cs, "ADDR", ipPort)}
}

func (c clientKillIpPort) Laddr(ipPort string) clientKillLaddr {
	return clientKillLaddr{cs: append(c.cs, "LADDR", ipPort)}
}

func (c clientKillIpPort) Skipme(yesNo string) clientKillSkipme {
	return clientKillSkipme{cs: append(c.cs, "SKIPME", yesNo)}
}

func (c clientKillIpPort) Build() []string {
	return c.cs
}

type clientKillLaddr struct {
	cs []string
}

func (c clientKillLaddr) Skipme(yesNo string) clientKillSkipme {
	return clientKillSkipme{cs: append(c.cs, "SKIPME", yesNo)}
}

func (c clientKillLaddr) Build() []string {
	return c.cs
}

type clientKillMaster struct {
	cs []string
}

func (c clientKillMaster) User(username string) clientKillUser {
	return clientKillUser{cs: append(c.cs, "USER", username)}
}

func (c clientKillMaster) Addr(ipPort string) clientKillAddr {
	return clientKillAddr{cs: append(c.cs, "ADDR", ipPort)}
}

func (c clientKillMaster) Laddr(ipPort string) clientKillLaddr {
	return clientKillLaddr{cs: append(c.cs, "LADDR", ipPort)}
}

func (c clientKillMaster) Skipme(yesNo string) clientKillSkipme {
	return clientKillSkipme{cs: append(c.cs, "SKIPME", yesNo)}
}

func (c clientKillMaster) Build() []string {
	return c.cs
}

type clientKillNormal struct {
	cs []string
}

func (c clientKillNormal) User(username string) clientKillUser {
	return clientKillUser{cs: append(c.cs, "USER", username)}
}

func (c clientKillNormal) Addr(ipPort string) clientKillAddr {
	return clientKillAddr{cs: append(c.cs, "ADDR", ipPort)}
}

func (c clientKillNormal) Laddr(ipPort string) clientKillLaddr {
	return clientKillLaddr{cs: append(c.cs, "LADDR", ipPort)}
}

func (c clientKillNormal) Skipme(yesNo string) clientKillSkipme {
	return clientKillSkipme{cs: append(c.cs, "SKIPME", yesNo)}
}

func (c clientKillNormal) Build() []string {
	return c.cs
}

type clientKillPubsub struct {
	cs []string
}

func (c clientKillPubsub) User(username string) clientKillUser {
	return clientKillUser{cs: append(c.cs, "USER", username)}
}

func (c clientKillPubsub) Addr(ipPort string) clientKillAddr {
	return clientKillAddr{cs: append(c.cs, "ADDR", ipPort)}
}

func (c clientKillPubsub) Laddr(ipPort string) clientKillLaddr {
	return clientKillLaddr{cs: append(c.cs, "LADDR", ipPort)}
}

func (c clientKillPubsub) Skipme(yesNo string) clientKillSkipme {
	return clientKillSkipme{cs: append(c.cs, "SKIPME", yesNo)}
}

func (c clientKillPubsub) Build() []string {
	return c.cs
}

type clientKillSkipme struct {
	cs []string
}

func (c clientKillSkipme) Build() []string {
	return c.cs
}

type clientKillSlave struct {
	cs []string
}

func (c clientKillSlave) User(username string) clientKillUser {
	return clientKillUser{cs: append(c.cs, "USER", username)}
}

func (c clientKillSlave) Addr(ipPort string) clientKillAddr {
	return clientKillAddr{cs: append(c.cs, "ADDR", ipPort)}
}

func (c clientKillSlave) Laddr(ipPort string) clientKillLaddr {
	return clientKillLaddr{cs: append(c.cs, "LADDR", ipPort)}
}

func (c clientKillSlave) Skipme(yesNo string) clientKillSkipme {
	return clientKillSkipme{cs: append(c.cs, "SKIPME", yesNo)}
}

func (c clientKillSlave) Build() []string {
	return c.cs
}

type clientKillUser struct {
	cs []string
}

func (c clientKillUser) Addr(ipPort string) clientKillAddr {
	return clientKillAddr{cs: append(c.cs, "ADDR", ipPort)}
}

func (c clientKillUser) Laddr(ipPort string) clientKillLaddr {
	return clientKillLaddr{cs: append(c.cs, "LADDR", ipPort)}
}

func (c clientKillUser) Skipme(yesNo string) clientKillSkipme {
	return clientKillSkipme{cs: append(c.cs, "SKIPME", yesNo)}
}

func (c clientKillUser) Build() []string {
	return c.cs
}

type clientList struct {
	cs []string
}

func (c clientList) Normal() clientListNormal {
	return clientListNormal{cs: append(c.cs, "normal")}
}

func (c clientList) Master() clientListMaster {
	return clientListMaster{cs: append(c.cs, "master")}
}

func (c clientList) Replica() clientListReplica {
	return clientListReplica{cs: append(c.cs, "replica")}
}

func (c clientList) Pubsub() clientListPubsub {
	return clientListPubsub{cs: append(c.cs, "pubsub")}
}

func (c clientList) Id() clientListIdId {
	return clientListIdId{cs: append(c.cs, "ID")}
}

func (b *Builder) ClientList() (c clientList) {
	c.cs = append(b.get(), "CLIENT", "LIST")
	return
}

type clientListIdClientId struct {
	cs []string
}

func (c clientListIdClientId) ClientId(clientId ...int64) clientListIdClientId {
	for _, n := range clientId {
		c.cs = append(c.cs, strconv.FormatInt(n, 10))
	}
	return clientListIdClientId{cs: c.cs}
}

func (c clientListIdClientId) Build() []string {
	return c.cs
}

type clientListIdId struct {
	cs []string
}

func (c clientListIdId) ClientId(clientId ...int64) clientListIdClientId {
	for _, n := range clientId {
		c.cs = append(c.cs, strconv.FormatInt(n, 10))
	}
	return clientListIdClientId{cs: c.cs}
}

type clientListMaster struct {
	cs []string
}

func (c clientListMaster) Id() clientListIdId {
	return clientListIdId{cs: append(c.cs, "ID")}
}

type clientListNormal struct {
	cs []string
}

func (c clientListNormal) Id() clientListIdId {
	return clientListIdId{cs: append(c.cs, "ID")}
}

type clientListPubsub struct {
	cs []string
}

func (c clientListPubsub) Id() clientListIdId {
	return clientListIdId{cs: append(c.cs, "ID")}
}

type clientListReplica struct {
	cs []string
}

func (c clientListReplica) Id() clientListIdId {
	return clientListIdId{cs: append(c.cs, "ID")}
}

type clientNoEvict struct {
	cs []string
}

func (c clientNoEvict) On() clientNoEvictEnabledOn {
	return clientNoEvictEnabledOn{cs: append(c.cs, "ON")}
}

func (c clientNoEvict) Off() clientNoEvictEnabledOff {
	return clientNoEvictEnabledOff{cs: append(c.cs, "OFF")}
}

func (b *Builder) ClientNoEvict() (c clientNoEvict) {
	c.cs = append(b.get(), "CLIENT", "NO-EVICT")
	return
}

type clientNoEvictEnabledOff struct {
	cs []string
}

func (c clientNoEvictEnabledOff) Build() []string {
	return c.cs
}

type clientNoEvictEnabledOn struct {
	cs []string
}

func (c clientNoEvictEnabledOn) Build() []string {
	return c.cs
}

type clientPause struct {
	cs []string
}

func (c clientPause) Timeout(timeout int64) clientPauseTimeout {
	return clientPauseTimeout{cs: append(c.cs, strconv.FormatInt(timeout, 10))}
}

func (b *Builder) ClientPause() (c clientPause) {
	c.cs = append(b.get(), "CLIENT", "PAUSE")
	return
}

type clientPauseModeAll struct {
	cs []string
}

func (c clientPauseModeAll) Build() []string {
	return c.cs
}

type clientPauseModeWrite struct {
	cs []string
}

func (c clientPauseModeWrite) Build() []string {
	return c.cs
}

type clientPauseTimeout struct {
	cs []string
}

func (c clientPauseTimeout) Write() clientPauseModeWrite {
	return clientPauseModeWrite{cs: append(c.cs, "WRITE")}
}

func (c clientPauseTimeout) All() clientPauseModeAll {
	return clientPauseModeAll{cs: append(c.cs, "ALL")}
}

func (c clientPauseTimeout) Build() []string {
	return c.cs
}

type clientReply struct {
	cs []string
}

func (c clientReply) On() clientReplyReplyModeOn {
	return clientReplyReplyModeOn{cs: append(c.cs, "ON")}
}

func (c clientReply) Off() clientReplyReplyModeOff {
	return clientReplyReplyModeOff{cs: append(c.cs, "OFF")}
}

func (c clientReply) Skip() clientReplyReplyModeSkip {
	return clientReplyReplyModeSkip{cs: append(c.cs, "SKIP")}
}

func (b *Builder) ClientReply() (c clientReply) {
	c.cs = append(b.get(), "CLIENT", "REPLY")
	return
}

type clientReplyReplyModeOff struct {
	cs []string
}

func (c clientReplyReplyModeOff) Build() []string {
	return c.cs
}

type clientReplyReplyModeOn struct {
	cs []string
}

func (c clientReplyReplyModeOn) Build() []string {
	return c.cs
}

type clientReplyReplyModeSkip struct {
	cs []string
}

func (c clientReplyReplyModeSkip) Build() []string {
	return c.cs
}

type clientSetname struct {
	cs []string
}

func (c clientSetname) ConnectionName(connectionName string) clientSetnameConnectionName {
	return clientSetnameConnectionName{cs: append(c.cs, connectionName)}
}

func (b *Builder) ClientSetname() (c clientSetname) {
	c.cs = append(b.get(), "CLIENT", "SETNAME")
	return
}

type clientSetnameConnectionName struct {
	cs []string
}

func (c clientSetnameConnectionName) Build() []string {
	return c.cs
}

type clientTracking struct {
	cs []string
}

func (c clientTracking) On() clientTrackingStatusOn {
	return clientTrackingStatusOn{cs: append(c.cs, "ON")}
}

func (c clientTracking) Off() clientTrackingStatusOff {
	return clientTrackingStatusOff{cs: append(c.cs, "OFF")}
}

func (b *Builder) ClientTracking() (c clientTracking) {
	c.cs = append(b.get(), "CLIENT", "TRACKING")
	return
}

type clientTrackingBcastBcast struct {
	cs []string
}

func (c clientTrackingBcastBcast) Optin() clientTrackingOptinOptin {
	return clientTrackingOptinOptin{cs: append(c.cs, "OPTIN")}
}

func (c clientTrackingBcastBcast) Optout() clientTrackingOptoutOptout {
	return clientTrackingOptoutOptout{cs: append(c.cs, "OPTOUT")}
}

func (c clientTrackingBcastBcast) Noloop() clientTrackingNoloopNoloop {
	return clientTrackingNoloopNoloop{cs: append(c.cs, "NOLOOP")}
}

func (c clientTrackingBcastBcast) Build() []string {
	return c.cs
}

type clientTrackingNoloopNoloop struct {
	cs []string
}

func (c clientTrackingNoloopNoloop) Build() []string {
	return c.cs
}

type clientTrackingOptinOptin struct {
	cs []string
}

func (c clientTrackingOptinOptin) Optout() clientTrackingOptoutOptout {
	return clientTrackingOptoutOptout{cs: append(c.cs, "OPTOUT")}
}

func (c clientTrackingOptinOptin) Noloop() clientTrackingNoloopNoloop {
	return clientTrackingNoloopNoloop{cs: append(c.cs, "NOLOOP")}
}

func (c clientTrackingOptinOptin) Build() []string {
	return c.cs
}

type clientTrackingOptoutOptout struct {
	cs []string
}

func (c clientTrackingOptoutOptout) Noloop() clientTrackingNoloopNoloop {
	return clientTrackingNoloopNoloop{cs: append(c.cs, "NOLOOP")}
}

func (c clientTrackingOptoutOptout) Build() []string {
	return c.cs
}

type clientTrackingPrefix struct {
	cs []string
}

func (c clientTrackingPrefix) Bcast() clientTrackingBcastBcast {
	return clientTrackingBcastBcast{cs: append(c.cs, "BCAST")}
}

func (c clientTrackingPrefix) Optin() clientTrackingOptinOptin {
	return clientTrackingOptinOptin{cs: append(c.cs, "OPTIN")}
}

func (c clientTrackingPrefix) Optout() clientTrackingOptoutOptout {
	return clientTrackingOptoutOptout{cs: append(c.cs, "OPTOUT")}
}

func (c clientTrackingPrefix) Noloop() clientTrackingNoloopNoloop {
	return clientTrackingNoloopNoloop{cs: append(c.cs, "NOLOOP")}
}

func (c clientTrackingPrefix) Prefix(prefix ...string) clientTrackingPrefix {
	return clientTrackingPrefix{cs: append(c.cs, prefix...)}
}

func (c clientTrackingPrefix) Build() []string {
	return c.cs
}

type clientTrackingRedirect struct {
	cs []string
}

func (c clientTrackingRedirect) Prefix(prefix ...string) clientTrackingPrefix {
	c.cs = append(c.cs, "PREFIX")
	return clientTrackingPrefix{cs: append(c.cs, prefix...)}
}

func (c clientTrackingRedirect) Bcast() clientTrackingBcastBcast {
	return clientTrackingBcastBcast{cs: append(c.cs, "BCAST")}
}

func (c clientTrackingRedirect) Optin() clientTrackingOptinOptin {
	return clientTrackingOptinOptin{cs: append(c.cs, "OPTIN")}
}

func (c clientTrackingRedirect) Optout() clientTrackingOptoutOptout {
	return clientTrackingOptoutOptout{cs: append(c.cs, "OPTOUT")}
}

func (c clientTrackingRedirect) Noloop() clientTrackingNoloopNoloop {
	return clientTrackingNoloopNoloop{cs: append(c.cs, "NOLOOP")}
}

func (c clientTrackingRedirect) Build() []string {
	return c.cs
}

type clientTrackingStatusOff struct {
	cs []string
}

func (c clientTrackingStatusOff) Redirect(clientId int64) clientTrackingRedirect {
	return clientTrackingRedirect{cs: append(c.cs, "REDIRECT", strconv.FormatInt(clientId, 10))}
}

func (c clientTrackingStatusOff) Prefix(prefix ...string) clientTrackingPrefix {
	c.cs = append(c.cs, "PREFIX")
	return clientTrackingPrefix{cs: append(c.cs, prefix...)}
}

func (c clientTrackingStatusOff) Bcast() clientTrackingBcastBcast {
	return clientTrackingBcastBcast{cs: append(c.cs, "BCAST")}
}

func (c clientTrackingStatusOff) Optin() clientTrackingOptinOptin {
	return clientTrackingOptinOptin{cs: append(c.cs, "OPTIN")}
}

func (c clientTrackingStatusOff) Optout() clientTrackingOptoutOptout {
	return clientTrackingOptoutOptout{cs: append(c.cs, "OPTOUT")}
}

func (c clientTrackingStatusOff) Noloop() clientTrackingNoloopNoloop {
	return clientTrackingNoloopNoloop{cs: append(c.cs, "NOLOOP")}
}

func (c clientTrackingStatusOff) Build() []string {
	return c.cs
}

type clientTrackingStatusOn struct {
	cs []string
}

func (c clientTrackingStatusOn) Redirect(clientId int64) clientTrackingRedirect {
	return clientTrackingRedirect{cs: append(c.cs, "REDIRECT", strconv.FormatInt(clientId, 10))}
}

func (c clientTrackingStatusOn) Prefix(prefix ...string) clientTrackingPrefix {
	c.cs = append(c.cs, "PREFIX")
	return clientTrackingPrefix{cs: append(c.cs, prefix...)}
}

func (c clientTrackingStatusOn) Bcast() clientTrackingBcastBcast {
	return clientTrackingBcastBcast{cs: append(c.cs, "BCAST")}
}

func (c clientTrackingStatusOn) Optin() clientTrackingOptinOptin {
	return clientTrackingOptinOptin{cs: append(c.cs, "OPTIN")}
}

func (c clientTrackingStatusOn) Optout() clientTrackingOptoutOptout {
	return clientTrackingOptoutOptout{cs: append(c.cs, "OPTOUT")}
}

func (c clientTrackingStatusOn) Noloop() clientTrackingNoloopNoloop {
	return clientTrackingNoloopNoloop{cs: append(c.cs, "NOLOOP")}
}

func (c clientTrackingStatusOn) Build() []string {
	return c.cs
}

type clientTrackinginfo struct {
	cs []string
}

func (c clientTrackinginfo) Build() []string {
	return c.cs
}

func (b *Builder) ClientTrackinginfo() (c clientTrackinginfo) {
	c.cs = append(b.get(), "CLIENT", "TRACKINGINFO")
	return
}

type clientUnblock struct {
	cs []string
}

func (c clientUnblock) ClientId(clientId int64) clientUnblockClientId {
	return clientUnblockClientId{cs: append(c.cs, strconv.FormatInt(clientId, 10))}
}

func (b *Builder) ClientUnblock() (c clientUnblock) {
	c.cs = append(b.get(), "CLIENT", "UNBLOCK")
	return
}

type clientUnblockClientId struct {
	cs []string
}

func (c clientUnblockClientId) Timeout() clientUnblockUnblockTypeTimeout {
	return clientUnblockUnblockTypeTimeout{cs: append(c.cs, "TIMEOUT")}
}

func (c clientUnblockClientId) Error() clientUnblockUnblockTypeError {
	return clientUnblockUnblockTypeError{cs: append(c.cs, "ERROR")}
}

func (c clientUnblockClientId) Build() []string {
	return c.cs
}

type clientUnblockUnblockTypeError struct {
	cs []string
}

func (c clientUnblockUnblockTypeError) Build() []string {
	return c.cs
}

type clientUnblockUnblockTypeTimeout struct {
	cs []string
}

func (c clientUnblockUnblockTypeTimeout) Build() []string {
	return c.cs
}

type clientUnpause struct {
	cs []string
}

func (c clientUnpause) Build() []string {
	return c.cs
}

func (b *Builder) ClientUnpause() (c clientUnpause) {
	c.cs = append(b.get(), "CLIENT", "UNPAUSE")
	return
}

type clusterAddslots struct {
	cs []string
}

func (c clusterAddslots) Slot(slot ...int64) clusterAddslotsSlot {
	for _, n := range slot {
		c.cs = append(c.cs, strconv.FormatInt(n, 10))
	}
	return clusterAddslotsSlot{cs: c.cs}
}

func (b *Builder) ClusterAddslots() (c clusterAddslots) {
	c.cs = append(b.get(), "CLUSTER", "ADDSLOTS")
	return
}

type clusterAddslotsSlot struct {
	cs []string
}

func (c clusterAddslotsSlot) Slot(slot ...int64) clusterAddslotsSlot {
	for _, n := range slot {
		c.cs = append(c.cs, strconv.FormatInt(n, 10))
	}
	return clusterAddslotsSlot{cs: c.cs}
}

func (c clusterAddslotsSlot) Build() []string {
	return c.cs
}

type clusterBumpepoch struct {
	cs []string
}

func (c clusterBumpepoch) Build() []string {
	return c.cs
}

func (b *Builder) ClusterBumpepoch() (c clusterBumpepoch) {
	c.cs = append(b.get(), "CLUSTER", "BUMPEPOCH")
	return
}

type clusterCountFailureReports struct {
	cs []string
}

func (c clusterCountFailureReports) NodeId(nodeId string) clusterCountFailureReportsNodeId {
	return clusterCountFailureReportsNodeId{cs: append(c.cs, nodeId)}
}

func (b *Builder) ClusterCountFailureReports() (c clusterCountFailureReports) {
	c.cs = append(b.get(), "CLUSTER", "COUNT-FAILURE-REPORTS")
	return
}

type clusterCountFailureReportsNodeId struct {
	cs []string
}

func (c clusterCountFailureReportsNodeId) Build() []string {
	return c.cs
}

type clusterCountkeysinslot struct {
	cs []string
}

func (c clusterCountkeysinslot) Slot(slot int64) clusterCountkeysinslotSlot {
	return clusterCountkeysinslotSlot{cs: append(c.cs, strconv.FormatInt(slot, 10))}
}

func (b *Builder) ClusterCountkeysinslot() (c clusterCountkeysinslot) {
	c.cs = append(b.get(), "CLUSTER", "COUNTKEYSINSLOT")
	return
}

type clusterCountkeysinslotSlot struct {
	cs []string
}

func (c clusterCountkeysinslotSlot) Build() []string {
	return c.cs
}

type clusterDelslots struct {
	cs []string
}

func (c clusterDelslots) Slot(slot ...int64) clusterDelslotsSlot {
	for _, n := range slot {
		c.cs = append(c.cs, strconv.FormatInt(n, 10))
	}
	return clusterDelslotsSlot{cs: c.cs}
}

func (b *Builder) ClusterDelslots() (c clusterDelslots) {
	c.cs = append(b.get(), "CLUSTER", "DELSLOTS")
	return
}

type clusterDelslotsSlot struct {
	cs []string
}

func (c clusterDelslotsSlot) Slot(slot ...int64) clusterDelslotsSlot {
	for _, n := range slot {
		c.cs = append(c.cs, strconv.FormatInt(n, 10))
	}
	return clusterDelslotsSlot{cs: c.cs}
}

func (c clusterDelslotsSlot) Build() []string {
	return c.cs
}

type clusterFailover struct {
	cs []string
}

func (c clusterFailover) Force() clusterFailoverOptionsForce {
	return clusterFailoverOptionsForce{cs: append(c.cs, "FORCE")}
}

func (c clusterFailover) Takeover() clusterFailoverOptionsTakeover {
	return clusterFailoverOptionsTakeover{cs: append(c.cs, "TAKEOVER")}
}

func (c clusterFailover) Build() []string {
	return c.cs
}

func (b *Builder) ClusterFailover() (c clusterFailover) {
	c.cs = append(b.get(), "CLUSTER", "FAILOVER")
	return
}

type clusterFailoverOptionsForce struct {
	cs []string
}

func (c clusterFailoverOptionsForce) Build() []string {
	return c.cs
}

type clusterFailoverOptionsTakeover struct {
	cs []string
}

func (c clusterFailoverOptionsTakeover) Build() []string {
	return c.cs
}

type clusterFlushslots struct {
	cs []string
}

func (c clusterFlushslots) Build() []string {
	return c.cs
}

func (b *Builder) ClusterFlushslots() (c clusterFlushslots) {
	c.cs = append(b.get(), "CLUSTER", "FLUSHSLOTS")
	return
}

type clusterForget struct {
	cs []string
}

func (c clusterForget) NodeId(nodeId string) clusterForgetNodeId {
	return clusterForgetNodeId{cs: append(c.cs, nodeId)}
}

func (b *Builder) ClusterForget() (c clusterForget) {
	c.cs = append(b.get(), "CLUSTER", "FORGET")
	return
}

type clusterForgetNodeId struct {
	cs []string
}

func (c clusterForgetNodeId) Build() []string {
	return c.cs
}

type clusterGetkeysinslot struct {
	cs []string
}

func (c clusterGetkeysinslot) Slot(slot int64) clusterGetkeysinslotSlot {
	return clusterGetkeysinslotSlot{cs: append(c.cs, strconv.FormatInt(slot, 10))}
}

func (b *Builder) ClusterGetkeysinslot() (c clusterGetkeysinslot) {
	c.cs = append(b.get(), "CLUSTER", "GETKEYSINSLOT")
	return
}

type clusterGetkeysinslotCount struct {
	cs []string
}

func (c clusterGetkeysinslotCount) Build() []string {
	return c.cs
}

type clusterGetkeysinslotSlot struct {
	cs []string
}

func (c clusterGetkeysinslotSlot) Count(count int64) clusterGetkeysinslotCount {
	return clusterGetkeysinslotCount{cs: append(c.cs, strconv.FormatInt(count, 10))}
}

type clusterInfo struct {
	cs []string
}

func (c clusterInfo) Build() []string {
	return c.cs
}

func (b *Builder) ClusterInfo() (c clusterInfo) {
	c.cs = append(b.get(), "CLUSTER", "INFO")
	return
}

type clusterKeyslot struct {
	cs []string
}

func (c clusterKeyslot) Key(key string) clusterKeyslotKey {
	return clusterKeyslotKey{cs: append(c.cs, key)}
}

func (b *Builder) ClusterKeyslot() (c clusterKeyslot) {
	c.cs = append(b.get(), "CLUSTER", "KEYSLOT")
	return
}

type clusterKeyslotKey struct {
	cs []string
}

func (c clusterKeyslotKey) Build() []string {
	return c.cs
}

type clusterMeet struct {
	cs []string
}

func (c clusterMeet) Ip(ip string) clusterMeetIp {
	return clusterMeetIp{cs: append(c.cs, ip)}
}

func (b *Builder) ClusterMeet() (c clusterMeet) {
	c.cs = append(b.get(), "CLUSTER", "MEET")
	return
}

type clusterMeetIp struct {
	cs []string
}

func (c clusterMeetIp) Port(port int64) clusterMeetPort {
	return clusterMeetPort{cs: append(c.cs, strconv.FormatInt(port, 10))}
}

type clusterMeetPort struct {
	cs []string
}

func (c clusterMeetPort) Build() []string {
	return c.cs
}

type clusterMyid struct {
	cs []string
}

func (c clusterMyid) Build() []string {
	return c.cs
}

func (b *Builder) ClusterMyid() (c clusterMyid) {
	c.cs = append(b.get(), "CLUSTER", "MYID")
	return
}

type clusterNodes struct {
	cs []string
}

func (c clusterNodes) Build() []string {
	return c.cs
}

func (b *Builder) ClusterNodes() (c clusterNodes) {
	c.cs = append(b.get(), "CLUSTER", "NODES")
	return
}

type clusterReplicas struct {
	cs []string
}

func (c clusterReplicas) NodeId(nodeId string) clusterReplicasNodeId {
	return clusterReplicasNodeId{cs: append(c.cs, nodeId)}
}

func (b *Builder) ClusterReplicas() (c clusterReplicas) {
	c.cs = append(b.get(), "CLUSTER", "REPLICAS")
	return
}

type clusterReplicasNodeId struct {
	cs []string
}

func (c clusterReplicasNodeId) Build() []string {
	return c.cs
}

type clusterReplicate struct {
	cs []string
}

func (c clusterReplicate) NodeId(nodeId string) clusterReplicateNodeId {
	return clusterReplicateNodeId{cs: append(c.cs, nodeId)}
}

func (b *Builder) ClusterReplicate() (c clusterReplicate) {
	c.cs = append(b.get(), "CLUSTER", "REPLICATE")
	return
}

type clusterReplicateNodeId struct {
	cs []string
}

func (c clusterReplicateNodeId) Build() []string {
	return c.cs
}

type clusterReset struct {
	cs []string
}

func (c clusterReset) Hard() clusterResetResetTypeHard {
	return clusterResetResetTypeHard{cs: append(c.cs, "HARD")}
}

func (c clusterReset) Soft() clusterResetResetTypeSoft {
	return clusterResetResetTypeSoft{cs: append(c.cs, "SOFT")}
}

func (c clusterReset) Build() []string {
	return c.cs
}

func (b *Builder) ClusterReset() (c clusterReset) {
	c.cs = append(b.get(), "CLUSTER", "RESET")
	return
}

type clusterResetResetTypeHard struct {
	cs []string
}

func (c clusterResetResetTypeHard) Build() []string {
	return c.cs
}

type clusterResetResetTypeSoft struct {
	cs []string
}

func (c clusterResetResetTypeSoft) Build() []string {
	return c.cs
}

type clusterSaveconfig struct {
	cs []string
}

func (c clusterSaveconfig) Build() []string {
	return c.cs
}

func (b *Builder) ClusterSaveconfig() (c clusterSaveconfig) {
	c.cs = append(b.get(), "CLUSTER", "SAVECONFIG")
	return
}

type clusterSetConfigEpoch struct {
	cs []string
}

func (c clusterSetConfigEpoch) ConfigEpoch(configEpoch int64) clusterSetConfigEpochConfigEpoch {
	return clusterSetConfigEpochConfigEpoch{cs: append(c.cs, strconv.FormatInt(configEpoch, 10))}
}

func (b *Builder) ClusterSetConfigEpoch() (c clusterSetConfigEpoch) {
	c.cs = append(b.get(), "CLUSTER", "SET-CONFIG-EPOCH")
	return
}

type clusterSetConfigEpochConfigEpoch struct {
	cs []string
}

func (c clusterSetConfigEpochConfigEpoch) Build() []string {
	return c.cs
}

type clusterSetslot struct {
	cs []string
}

func (c clusterSetslot) Slot(slot int64) clusterSetslotSlot {
	return clusterSetslotSlot{cs: append(c.cs, strconv.FormatInt(slot, 10))}
}

func (b *Builder) ClusterSetslot() (c clusterSetslot) {
	c.cs = append(b.get(), "CLUSTER", "SETSLOT")
	return
}

type clusterSetslotNodeId struct {
	cs []string
}

func (c clusterSetslotNodeId) Build() []string {
	return c.cs
}

type clusterSetslotSlot struct {
	cs []string
}

func (c clusterSetslotSlot) Importing() clusterSetslotSubcommandImporting {
	return clusterSetslotSubcommandImporting{cs: append(c.cs, "IMPORTING")}
}

func (c clusterSetslotSlot) Migrating() clusterSetslotSubcommandMigrating {
	return clusterSetslotSubcommandMigrating{cs: append(c.cs, "MIGRATING")}
}

func (c clusterSetslotSlot) Stable() clusterSetslotSubcommandStable {
	return clusterSetslotSubcommandStable{cs: append(c.cs, "STABLE")}
}

func (c clusterSetslotSlot) Node() clusterSetslotSubcommandNode {
	return clusterSetslotSubcommandNode{cs: append(c.cs, "NODE")}
}

type clusterSetslotSubcommandImporting struct {
	cs []string
}

func (c clusterSetslotSubcommandImporting) NodeId(nodeId string) clusterSetslotNodeId {
	return clusterSetslotNodeId{cs: append(c.cs, nodeId)}
}

func (c clusterSetslotSubcommandImporting) Build() []string {
	return c.cs
}

type clusterSetslotSubcommandMigrating struct {
	cs []string
}

func (c clusterSetslotSubcommandMigrating) NodeId(nodeId string) clusterSetslotNodeId {
	return clusterSetslotNodeId{cs: append(c.cs, nodeId)}
}

func (c clusterSetslotSubcommandMigrating) Build() []string {
	return c.cs
}

type clusterSetslotSubcommandNode struct {
	cs []string
}

func (c clusterSetslotSubcommandNode) NodeId(nodeId string) clusterSetslotNodeId {
	return clusterSetslotNodeId{cs: append(c.cs, nodeId)}
}

func (c clusterSetslotSubcommandNode) Build() []string {
	return c.cs
}

type clusterSetslotSubcommandStable struct {
	cs []string
}

func (c clusterSetslotSubcommandStable) NodeId(nodeId string) clusterSetslotNodeId {
	return clusterSetslotNodeId{cs: append(c.cs, nodeId)}
}

func (c clusterSetslotSubcommandStable) Build() []string {
	return c.cs
}

type clusterSlaves struct {
	cs []string
}

func (c clusterSlaves) NodeId(nodeId string) clusterSlavesNodeId {
	return clusterSlavesNodeId{cs: append(c.cs, nodeId)}
}

func (b *Builder) ClusterSlaves() (c clusterSlaves) {
	c.cs = append(b.get(), "CLUSTER", "SLAVES")
	return
}

type clusterSlavesNodeId struct {
	cs []string
}

func (c clusterSlavesNodeId) Build() []string {
	return c.cs
}

type clusterSlots struct {
	cs []string
}

func (c clusterSlots) Build() []string {
	return c.cs
}

func (b *Builder) ClusterSlots() (c clusterSlots) {
	c.cs = append(b.get(), "CLUSTER", "SLOTS")
	return
}

type command struct {
	cs []string
}

func (c command) Build() []string {
	return c.cs
}

func (b *Builder) Command() (c command) {
	c.cs = append(b.get(), "COMMAND")
	return
}

type commandCount struct {
	cs []string
}

func (c commandCount) Build() []string {
	return c.cs
}

func (b *Builder) CommandCount() (c commandCount) {
	c.cs = append(b.get(), "COMMAND", "COUNT")
	return
}

type commandGetkeys struct {
	cs []string
}

func (c commandGetkeys) Build() []string {
	return c.cs
}

func (b *Builder) CommandGetkeys() (c commandGetkeys) {
	c.cs = append(b.get(), "COMMAND", "GETKEYS")
	return
}

type commandInfo struct {
	cs []string
}

func (c commandInfo) CommandName(commandName ...string) commandInfoCommandName {
	return commandInfoCommandName{cs: append(c.cs, commandName...)}
}

func (b *Builder) CommandInfo() (c commandInfo) {
	c.cs = append(b.get(), "COMMAND", "INFO")
	return
}

type commandInfoCommandName struct {
	cs []string
}

func (c commandInfoCommandName) CommandName(commandName ...string) commandInfoCommandName {
	return commandInfoCommandName{cs: append(c.cs, commandName...)}
}

func (c commandInfoCommandName) Build() []string {
	return c.cs
}

type configGet struct {
	cs []string
}

func (c configGet) Parameter(parameter string) configGetParameter {
	return configGetParameter{cs: append(c.cs, parameter)}
}

func (b *Builder) ConfigGet() (c configGet) {
	c.cs = append(b.get(), "CONFIG", "GET")
	return
}

type configGetParameter struct {
	cs []string
}

func (c configGetParameter) Build() []string {
	return c.cs
}

type configResetstat struct {
	cs []string
}

func (c configResetstat) Build() []string {
	return c.cs
}

func (b *Builder) ConfigResetstat() (c configResetstat) {
	c.cs = append(b.get(), "CONFIG", "RESETSTAT")
	return
}

type configRewrite struct {
	cs []string
}

func (c configRewrite) Build() []string {
	return c.cs
}

func (b *Builder) ConfigRewrite() (c configRewrite) {
	c.cs = append(b.get(), "CONFIG", "REWRITE")
	return
}

type configSet struct {
	cs []string
}

func (c configSet) Parameter(parameter string) configSetParameter {
	return configSetParameter{cs: append(c.cs, parameter)}
}

func (b *Builder) ConfigSet() (c configSet) {
	c.cs = append(b.get(), "CONFIG", "SET")
	return
}

type configSetParameter struct {
	cs []string
}

func (c configSetParameter) Value(value string) configSetValue {
	return configSetValue{cs: append(c.cs, value)}
}

type configSetValue struct {
	cs []string
}

func (c configSetValue) Build() []string {
	return c.cs
}

type copy struct {
	cs []string
}

func (c copy) Source(source string) copySource {
	return copySource{cs: append(c.cs, source)}
}

func (b *Builder) Copy() (c copy) {
	c.cs = append(b.get(), "COPY")
	return
}

type copyDb struct {
	cs []string
}

func (c copyDb) Replace() copyReplaceReplace {
	return copyReplaceReplace{cs: append(c.cs, "REPLACE")}
}

func (c copyDb) Build() []string {
	return c.cs
}

type copyDestination struct {
	cs []string
}

func (c copyDestination) Db(destinationDb int64) copyDb {
	return copyDb{cs: append(c.cs, "DB", strconv.FormatInt(destinationDb, 10))}
}

func (c copyDestination) Replace() copyReplaceReplace {
	return copyReplaceReplace{cs: append(c.cs, "REPLACE")}
}

func (c copyDestination) Build() []string {
	return c.cs
}

type copyReplaceReplace struct {
	cs []string
}

func (c copyReplaceReplace) Build() []string {
	return c.cs
}

type copySource struct {
	cs []string
}

func (c copySource) Destination(destination string) copyDestination {
	return copyDestination{cs: append(c.cs, destination)}
}

type dbsize struct {
	cs []string
}

func (c dbsize) Build() []string {
	return c.cs
}

func (b *Builder) Dbsize() (c dbsize) {
	c.cs = append(b.get(), "DBSIZE")
	return
}

type debugObject struct {
	cs []string
}

func (c debugObject) Key(key string) debugObjectKey {
	return debugObjectKey{cs: append(c.cs, key)}
}

func (b *Builder) DebugObject() (c debugObject) {
	c.cs = append(b.get(), "DEBUG", "OBJECT")
	return
}

type debugObjectKey struct {
	cs []string
}

func (c debugObjectKey) Build() []string {
	return c.cs
}

type debugSegfault struct {
	cs []string
}

func (c debugSegfault) Build() []string {
	return c.cs
}

func (b *Builder) DebugSegfault() (c debugSegfault) {
	c.cs = append(b.get(), "DEBUG", "SEGFAULT")
	return
}

type decr struct {
	cs []string
}

func (c decr) Key(key string) decrKey {
	return decrKey{cs: append(c.cs, key)}
}

func (b *Builder) Decr() (c decr) {
	c.cs = append(b.get(), "DECR")
	return
}

type decrKey struct {
	cs []string
}

func (c decrKey) Build() []string {
	return c.cs
}

type decrby struct {
	cs []string
}

func (c decrby) Key(key string) decrbyKey {
	return decrbyKey{cs: append(c.cs, key)}
}

func (b *Builder) Decrby() (c decrby) {
	c.cs = append(b.get(), "DECRBY")
	return
}

type decrbyDecrement struct {
	cs []string
}

func (c decrbyDecrement) Build() []string {
	return c.cs
}

type decrbyKey struct {
	cs []string
}

func (c decrbyKey) Decrement(decrement int64) decrbyDecrement {
	return decrbyDecrement{cs: append(c.cs, strconv.FormatInt(decrement, 10))}
}

type del struct {
	cs []string
}

func (c del) Key(key ...string) delKey {
	return delKey{cs: append(c.cs, key...)}
}

func (b *Builder) Del() (c del) {
	c.cs = append(b.get(), "DEL")
	return
}

type delKey struct {
	cs []string
}

func (c delKey) Key(key ...string) delKey {
	return delKey{cs: append(c.cs, key...)}
}

func (c delKey) Build() []string {
	return c.cs
}

type discard struct {
	cs []string
}

func (c discard) Build() []string {
	return c.cs
}

func (b *Builder) Discard() (c discard) {
	c.cs = append(b.get(), "DISCARD")
	return
}

type dump struct {
	cs []string
}

func (c dump) Key(key string) dumpKey {
	return dumpKey{cs: append(c.cs, key)}
}

func (b *Builder) Dump() (c dump) {
	c.cs = append(b.get(), "DUMP")
	return
}

type dumpKey struct {
	cs []string
}

func (c dumpKey) Build() []string {
	return c.cs
}

type echo struct {
	cs []string
}

func (c echo) Message(message string) echoMessage {
	return echoMessage{cs: append(c.cs, message)}
}

func (b *Builder) Echo() (c echo) {
	c.cs = append(b.get(), "ECHO")
	return
}

type echoMessage struct {
	cs []string
}

func (c echoMessage) Build() []string {
	return c.cs
}

type eval struct {
	cs []string
}

func (c eval) Script(script string) evalScript {
	return evalScript{cs: append(c.cs, script)}
}

func (b *Builder) Eval() (c eval) {
	c.cs = append(b.get(), "EVAL")
	return
}

type evalArg struct {
	cs []string
}

func (c evalArg) Arg(arg ...string) evalArg {
	return evalArg{cs: append(c.cs, arg...)}
}

func (c evalArg) Build() []string {
	return c.cs
}

type evalKey struct {
	cs []string
}

func (c evalKey) Arg(arg ...string) evalArg {
	return evalArg{cs: append(c.cs, arg...)}
}

func (c evalKey) Key(key ...string) evalKey {
	return evalKey{cs: append(c.cs, key...)}
}

func (c evalKey) Build() []string {
	return c.cs
}

type evalNumkeys struct {
	cs []string
}

func (c evalNumkeys) Key(key ...string) evalKey {
	return evalKey{cs: append(c.cs, key...)}
}

func (c evalNumkeys) Arg(arg ...string) evalArg {
	return evalArg{cs: append(c.cs, arg...)}
}

func (c evalNumkeys) Build() []string {
	return c.cs
}

type evalRo struct {
	cs []string
}

func (c evalRo) Script(script string) evalRoScript {
	return evalRoScript{cs: append(c.cs, script)}
}

func (b *Builder) EvalRo() (c evalRo) {
	c.cs = append(b.get(), "EVAL_RO")
	return
}

type evalRoArg struct {
	cs []string
}

func (c evalRoArg) Arg(arg ...string) evalRoArg {
	return evalRoArg{cs: append(c.cs, arg...)}
}

func (c evalRoArg) Build() []string {
	return c.cs
}

type evalRoKey struct {
	cs []string
}

func (c evalRoKey) Arg(arg ...string) evalRoArg {
	return evalRoArg{cs: append(c.cs, arg...)}
}

func (c evalRoKey) Key(key ...string) evalRoKey {
	return evalRoKey{cs: append(c.cs, key...)}
}

type evalRoNumkeys struct {
	cs []string
}

func (c evalRoNumkeys) Key(key ...string) evalRoKey {
	return evalRoKey{cs: append(c.cs, key...)}
}

type evalRoScript struct {
	cs []string
}

func (c evalRoScript) Numkeys(numkeys int64) evalRoNumkeys {
	return evalRoNumkeys{cs: append(c.cs, strconv.FormatInt(numkeys, 10))}
}

type evalScript struct {
	cs []string
}

func (c evalScript) Numkeys(numkeys int64) evalNumkeys {
	return evalNumkeys{cs: append(c.cs, strconv.FormatInt(numkeys, 10))}
}

type evalsha struct {
	cs []string
}

func (c evalsha) Sha1(sha1 string) evalshaSha1 {
	return evalshaSha1{cs: append(c.cs, sha1)}
}

func (b *Builder) Evalsha() (c evalsha) {
	c.cs = append(b.get(), "EVALSHA")
	return
}

type evalshaArg struct {
	cs []string
}

func (c evalshaArg) Arg(arg ...string) evalshaArg {
	return evalshaArg{cs: append(c.cs, arg...)}
}

func (c evalshaArg) Build() []string {
	return c.cs
}

type evalshaKey struct {
	cs []string
}

func (c evalshaKey) Arg(arg ...string) evalshaArg {
	return evalshaArg{cs: append(c.cs, arg...)}
}

func (c evalshaKey) Key(key ...string) evalshaKey {
	return evalshaKey{cs: append(c.cs, key...)}
}

func (c evalshaKey) Build() []string {
	return c.cs
}

type evalshaNumkeys struct {
	cs []string
}

func (c evalshaNumkeys) Key(key ...string) evalshaKey {
	return evalshaKey{cs: append(c.cs, key...)}
}

func (c evalshaNumkeys) Arg(arg ...string) evalshaArg {
	return evalshaArg{cs: append(c.cs, arg...)}
}

func (c evalshaNumkeys) Build() []string {
	return c.cs
}

type evalshaRo struct {
	cs []string
}

func (c evalshaRo) Sha1(sha1 string) evalshaRoSha1 {
	return evalshaRoSha1{cs: append(c.cs, sha1)}
}

func (b *Builder) EvalshaRo() (c evalshaRo) {
	c.cs = append(b.get(), "EVALSHA_RO")
	return
}

type evalshaRoArg struct {
	cs []string
}

func (c evalshaRoArg) Arg(arg ...string) evalshaRoArg {
	return evalshaRoArg{cs: append(c.cs, arg...)}
}

func (c evalshaRoArg) Build() []string {
	return c.cs
}

type evalshaRoKey struct {
	cs []string
}

func (c evalshaRoKey) Arg(arg ...string) evalshaRoArg {
	return evalshaRoArg{cs: append(c.cs, arg...)}
}

func (c evalshaRoKey) Key(key ...string) evalshaRoKey {
	return evalshaRoKey{cs: append(c.cs, key...)}
}

type evalshaRoNumkeys struct {
	cs []string
}

func (c evalshaRoNumkeys) Key(key ...string) evalshaRoKey {
	return evalshaRoKey{cs: append(c.cs, key...)}
}

type evalshaRoSha1 struct {
	cs []string
}

func (c evalshaRoSha1) Numkeys(numkeys int64) evalshaRoNumkeys {
	return evalshaRoNumkeys{cs: append(c.cs, strconv.FormatInt(numkeys, 10))}
}

type evalshaSha1 struct {
	cs []string
}

func (c evalshaSha1) Numkeys(numkeys int64) evalshaNumkeys {
	return evalshaNumkeys{cs: append(c.cs, strconv.FormatInt(numkeys, 10))}
}

type exec struct {
	cs []string
}

func (c exec) Build() []string {
	return c.cs
}

func (b *Builder) Exec() (c exec) {
	c.cs = append(b.get(), "EXEC")
	return
}

type exists struct {
	cs []string
}

func (c exists) Key(key ...string) existsKey {
	return existsKey{cs: append(c.cs, key...)}
}

func (b *Builder) Exists() (c exists) {
	c.cs = append(b.get(), "EXISTS")
	return
}

type existsKey struct {
	cs []string
}

func (c existsKey) Key(key ...string) existsKey {
	return existsKey{cs: append(c.cs, key...)}
}

func (c existsKey) Build() []string {
	return c.cs
}

type expire struct {
	cs []string
}

func (c expire) Key(key string) expireKey {
	return expireKey{cs: append(c.cs, key)}
}

func (b *Builder) Expire() (c expire) {
	c.cs = append(b.get(), "EXPIRE")
	return
}

type expireConditionGt struct {
	cs []string
}

func (c expireConditionGt) Build() []string {
	return c.cs
}

type expireConditionLt struct {
	cs []string
}

func (c expireConditionLt) Build() []string {
	return c.cs
}

type expireConditionNx struct {
	cs []string
}

func (c expireConditionNx) Build() []string {
	return c.cs
}

type expireConditionXx struct {
	cs []string
}

func (c expireConditionXx) Build() []string {
	return c.cs
}

type expireKey struct {
	cs []string
}

func (c expireKey) Seconds(seconds int64) expireSeconds {
	return expireSeconds{cs: append(c.cs, strconv.FormatInt(seconds, 10))}
}

type expireSeconds struct {
	cs []string
}

func (c expireSeconds) Nx() expireConditionNx {
	return expireConditionNx{cs: append(c.cs, "NX")}
}

func (c expireSeconds) Xx() expireConditionXx {
	return expireConditionXx{cs: append(c.cs, "XX")}
}

func (c expireSeconds) Gt() expireConditionGt {
	return expireConditionGt{cs: append(c.cs, "GT")}
}

func (c expireSeconds) Lt() expireConditionLt {
	return expireConditionLt{cs: append(c.cs, "LT")}
}

func (c expireSeconds) Build() []string {
	return c.cs
}

type expireat struct {
	cs []string
}

func (c expireat) Key(key string) expireatKey {
	return expireatKey{cs: append(c.cs, key)}
}

func (b *Builder) Expireat() (c expireat) {
	c.cs = append(b.get(), "EXPIREAT")
	return
}

type expireatConditionGt struct {
	cs []string
}

func (c expireatConditionGt) Build() []string {
	return c.cs
}

type expireatConditionLt struct {
	cs []string
}

func (c expireatConditionLt) Build() []string {
	return c.cs
}

type expireatConditionNx struct {
	cs []string
}

func (c expireatConditionNx) Build() []string {
	return c.cs
}

type expireatConditionXx struct {
	cs []string
}

func (c expireatConditionXx) Build() []string {
	return c.cs
}

type expireatKey struct {
	cs []string
}

func (c expireatKey) Timestamp(timestamp int64) expireatTimestamp {
	return expireatTimestamp{cs: append(c.cs, strconv.FormatInt(timestamp, 10))}
}

type expireatTimestamp struct {
	cs []string
}

func (c expireatTimestamp) Nx() expireatConditionNx {
	return expireatConditionNx{cs: append(c.cs, "NX")}
}

func (c expireatTimestamp) Xx() expireatConditionXx {
	return expireatConditionXx{cs: append(c.cs, "XX")}
}

func (c expireatTimestamp) Gt() expireatConditionGt {
	return expireatConditionGt{cs: append(c.cs, "GT")}
}

func (c expireatTimestamp) Lt() expireatConditionLt {
	return expireatConditionLt{cs: append(c.cs, "LT")}
}

func (c expireatTimestamp) Build() []string {
	return c.cs
}

type expiretime struct {
	cs []string
}

func (c expiretime) Key(key string) expiretimeKey {
	return expiretimeKey{cs: append(c.cs, key)}
}

func (b *Builder) Expiretime() (c expiretime) {
	c.cs = append(b.get(), "EXPIRETIME")
	return
}

type expiretimeKey struct {
	cs []string
}

func (c expiretimeKey) Build() []string {
	return c.cs
}

type failover struct {
	cs []string
}

func (c failover) To() failoverTargetTo {
	return failoverTargetTo{cs: append(c.cs, "TO")}
}

func (c failover) Abort() failoverAbort {
	return failoverAbort{cs: append(c.cs, "ABORT")}
}

func (c failover) Timeout(milliseconds int64) failoverTimeout {
	return failoverTimeout{cs: append(c.cs, "TIMEOUT", strconv.FormatInt(milliseconds, 10))}
}

func (b *Builder) Failover() (c failover) {
	c.cs = append(b.get(), "FAILOVER")
	return
}

type failoverAbort struct {
	cs []string
}

func (c failoverAbort) Timeout(milliseconds int64) failoverTimeout {
	return failoverTimeout{cs: append(c.cs, "TIMEOUT", strconv.FormatInt(milliseconds, 10))}
}

func (c failoverAbort) Build() []string {
	return c.cs
}

type failoverTargetForce struct {
	cs []string
}

func (c failoverTargetForce) Abort() failoverAbort {
	return failoverAbort{cs: append(c.cs, "ABORT")}
}

func (c failoverTargetForce) Timeout(milliseconds int64) failoverTimeout {
	return failoverTimeout{cs: append(c.cs, "TIMEOUT", strconv.FormatInt(milliseconds, 10))}
}

func (c failoverTargetForce) Build() []string {
	return c.cs
}

type failoverTargetHost struct {
	cs []string
}

func (c failoverTargetHost) Port(port int64) failoverTargetPort {
	return failoverTargetPort{cs: append(c.cs, strconv.FormatInt(port, 10))}
}

type failoverTargetPort struct {
	cs []string
}

func (c failoverTargetPort) Force() failoverTargetForce {
	return failoverTargetForce{cs: append(c.cs, "FORCE")}
}

func (c failoverTargetPort) Abort() failoverAbort {
	return failoverAbort{cs: append(c.cs, "ABORT")}
}

func (c failoverTargetPort) Timeout(milliseconds int64) failoverTimeout {
	return failoverTimeout{cs: append(c.cs, "TIMEOUT", strconv.FormatInt(milliseconds, 10))}
}

func (c failoverTargetPort) Build() []string {
	return c.cs
}

type failoverTargetTo struct {
	cs []string
}

func (c failoverTargetTo) Host(host string) failoverTargetHost {
	return failoverTargetHost{cs: append(c.cs, host)}
}

type failoverTimeout struct {
	cs []string
}

func (c failoverTimeout) Build() []string {
	return c.cs
}

type flushall struct {
	cs []string
}

func (c flushall) Async() flushallAsyncAsync {
	return flushallAsyncAsync{cs: append(c.cs, "ASYNC")}
}

func (c flushall) Sync() flushallAsyncSync {
	return flushallAsyncSync{cs: append(c.cs, "SYNC")}
}

func (c flushall) Build() []string {
	return c.cs
}

func (b *Builder) Flushall() (c flushall) {
	c.cs = append(b.get(), "FLUSHALL")
	return
}

type flushallAsyncAsync struct {
	cs []string
}

func (c flushallAsyncAsync) Build() []string {
	return c.cs
}

type flushallAsyncSync struct {
	cs []string
}

func (c flushallAsyncSync) Build() []string {
	return c.cs
}

type flushdb struct {
	cs []string
}

func (c flushdb) Async() flushdbAsyncAsync {
	return flushdbAsyncAsync{cs: append(c.cs, "ASYNC")}
}

func (c flushdb) Sync() flushdbAsyncSync {
	return flushdbAsyncSync{cs: append(c.cs, "SYNC")}
}

func (c flushdb) Build() []string {
	return c.cs
}

func (b *Builder) Flushdb() (c flushdb) {
	c.cs = append(b.get(), "FLUSHDB")
	return
}

type flushdbAsyncAsync struct {
	cs []string
}

func (c flushdbAsyncAsync) Build() []string {
	return c.cs
}

type flushdbAsyncSync struct {
	cs []string
}

func (c flushdbAsyncSync) Build() []string {
	return c.cs
}

type geoadd struct {
	cs []string
}

func (c geoadd) Key(key string) geoaddKey {
	return geoaddKey{cs: append(c.cs, key)}
}

func (b *Builder) Geoadd() (c geoadd) {
	c.cs = append(b.get(), "GEOADD")
	return
}

type geoaddChangeCh struct {
	cs []string
}

func (c geoaddChangeCh) LongitudeLatitudeMember() geoaddLongitudeLatitudeMember {
	return geoaddLongitudeLatitudeMember{cs: append(c.cs)}
}

type geoaddConditionNx struct {
	cs []string
}

func (c geoaddConditionNx) Ch() geoaddChangeCh {
	return geoaddChangeCh{cs: append(c.cs, "CH")}
}

func (c geoaddConditionNx) LongitudeLatitudeMember() geoaddLongitudeLatitudeMember {
	return geoaddLongitudeLatitudeMember{cs: append(c.cs)}
}

type geoaddConditionXx struct {
	cs []string
}

func (c geoaddConditionXx) Ch() geoaddChangeCh {
	return geoaddChangeCh{cs: append(c.cs, "CH")}
}

func (c geoaddConditionXx) LongitudeLatitudeMember() geoaddLongitudeLatitudeMember {
	return geoaddLongitudeLatitudeMember{cs: append(c.cs)}
}

type geoaddKey struct {
	cs []string
}

func (c geoaddKey) Nx() geoaddConditionNx {
	return geoaddConditionNx{cs: append(c.cs, "NX")}
}

func (c geoaddKey) Xx() geoaddConditionXx {
	return geoaddConditionXx{cs: append(c.cs, "XX")}
}

func (c geoaddKey) Ch() geoaddChangeCh {
	return geoaddChangeCh{cs: append(c.cs, "CH")}
}

func (c geoaddKey) LongitudeLatitudeMember() geoaddLongitudeLatitudeMember {
	return geoaddLongitudeLatitudeMember{cs: append(c.cs)}
}

type geoaddLongitudeLatitudeMember struct {
	cs []string
}

func (c geoaddLongitudeLatitudeMember) LongitudeLatitudeMember(longitude float64, latitude float64, member string) geoaddLongitudeLatitudeMember {
	return geoaddLongitudeLatitudeMember{cs: append(c.cs, strconv.FormatFloat(longitude, 'f', -1, 64), strconv.FormatFloat(latitude, 'f', -1, 64), member)}
}

func (c geoaddLongitudeLatitudeMember) Build() []string {
	return c.cs
}

type geodist struct {
	cs []string
}

func (c geodist) Key(key string) geodistKey {
	return geodistKey{cs: append(c.cs, key)}
}

func (b *Builder) Geodist() (c geodist) {
	c.cs = append(b.get(), "GEODIST")
	return
}

type geodistKey struct {
	cs []string
}

func (c geodistKey) Member1(member1 string) geodistMember1 {
	return geodistMember1{cs: append(c.cs, member1)}
}

type geodistMember1 struct {
	cs []string
}

func (c geodistMember1) Member2(member2 string) geodistMember2 {
	return geodistMember2{cs: append(c.cs, member2)}
}

type geodistMember2 struct {
	cs []string
}

func (c geodistMember2) M() geodistUnitM {
	return geodistUnitM{cs: append(c.cs, "m")}
}

func (c geodistMember2) Km() geodistUnitKm {
	return geodistUnitKm{cs: append(c.cs, "km")}
}

func (c geodistMember2) Ft() geodistUnitFt {
	return geodistUnitFt{cs: append(c.cs, "ft")}
}

func (c geodistMember2) Mi() geodistUnitMi {
	return geodistUnitMi{cs: append(c.cs, "mi")}
}

func (c geodistMember2) Build() []string {
	return c.cs
}

type geodistUnitFt struct {
	cs []string
}

func (c geodistUnitFt) Build() []string {
	return c.cs
}

type geodistUnitKm struct {
	cs []string
}

func (c geodistUnitKm) Build() []string {
	return c.cs
}

type geodistUnitM struct {
	cs []string
}

func (c geodistUnitM) Build() []string {
	return c.cs
}

type geodistUnitMi struct {
	cs []string
}

func (c geodistUnitMi) Build() []string {
	return c.cs
}

type geohash struct {
	cs []string
}

func (c geohash) Key(key string) geohashKey {
	return geohashKey{cs: append(c.cs, key)}
}

func (b *Builder) Geohash() (c geohash) {
	c.cs = append(b.get(), "GEOHASH")
	return
}

type geohashKey struct {
	cs []string
}

func (c geohashKey) Member(member ...string) geohashMember {
	return geohashMember{cs: append(c.cs, member...)}
}

type geohashMember struct {
	cs []string
}

func (c geohashMember) Member(member ...string) geohashMember {
	return geohashMember{cs: append(c.cs, member...)}
}

func (c geohashMember) Build() []string {
	return c.cs
}

type geopos struct {
	cs []string
}

func (c geopos) Key(key string) geoposKey {
	return geoposKey{cs: append(c.cs, key)}
}

func (b *Builder) Geopos() (c geopos) {
	c.cs = append(b.get(), "GEOPOS")
	return
}

type geoposKey struct {
	cs []string
}

func (c geoposKey) Member(member ...string) geoposMember {
	return geoposMember{cs: append(c.cs, member...)}
}

type geoposMember struct {
	cs []string
}

func (c geoposMember) Member(member ...string) geoposMember {
	return geoposMember{cs: append(c.cs, member...)}
}

func (c geoposMember) Build() []string {
	return c.cs
}

type georadius struct {
	cs []string
}

func (c georadius) Key(key string) georadiusKey {
	return georadiusKey{cs: append(c.cs, key)}
}

func (b *Builder) Georadius() (c georadius) {
	c.cs = append(b.get(), "GEORADIUS")
	return
}

type georadiusCountAnyAny struct {
	cs []string
}

func (c georadiusCountAnyAny) Asc() georadiusOrderAsc {
	return georadiusOrderAsc{cs: append(c.cs, "ASC")}
}

func (c georadiusCountAnyAny) Desc() georadiusOrderDesc {
	return georadiusOrderDesc{cs: append(c.cs, "DESC")}
}

func (c georadiusCountAnyAny) Store(key string) georadiusStore {
	return georadiusStore{cs: append(c.cs, "STORE", key)}
}

func (c georadiusCountAnyAny) Storedist(key string) georadiusStoredist {
	return georadiusStoredist{cs: append(c.cs, "STOREDIST", key)}
}

func (c georadiusCountAnyAny) Build() []string {
	return c.cs
}

type georadiusCountCount struct {
	cs []string
}

func (c georadiusCountCount) Any() georadiusCountAnyAny {
	return georadiusCountAnyAny{cs: append(c.cs, "ANY")}
}

func (c georadiusCountCount) Asc() georadiusOrderAsc {
	return georadiusOrderAsc{cs: append(c.cs, "ASC")}
}

func (c georadiusCountCount) Desc() georadiusOrderDesc {
	return georadiusOrderDesc{cs: append(c.cs, "DESC")}
}

func (c georadiusCountCount) Store(key string) georadiusStore {
	return georadiusStore{cs: append(c.cs, "STORE", key)}
}

func (c georadiusCountCount) Storedist(key string) georadiusStoredist {
	return georadiusStoredist{cs: append(c.cs, "STOREDIST", key)}
}

func (c georadiusCountCount) Build() []string {
	return c.cs
}

type georadiusKey struct {
	cs []string
}

func (c georadiusKey) Longitude(longitude float64) georadiusLongitude {
	return georadiusLongitude{cs: append(c.cs, strconv.FormatFloat(longitude, 'f', -1, 64))}
}

type georadiusLatitude struct {
	cs []string
}

func (c georadiusLatitude) Radius(radius float64) georadiusRadius {
	return georadiusRadius{cs: append(c.cs, strconv.FormatFloat(radius, 'f', -1, 64))}
}

type georadiusLongitude struct {
	cs []string
}

func (c georadiusLongitude) Latitude(latitude float64) georadiusLatitude {
	return georadiusLatitude{cs: append(c.cs, strconv.FormatFloat(latitude, 'f', -1, 64))}
}

type georadiusOrderAsc struct {
	cs []string
}

func (c georadiusOrderAsc) Store(key string) georadiusStore {
	return georadiusStore{cs: append(c.cs, "STORE", key)}
}

func (c georadiusOrderAsc) Storedist(key string) georadiusStoredist {
	return georadiusStoredist{cs: append(c.cs, "STOREDIST", key)}
}

func (c georadiusOrderAsc) Build() []string {
	return c.cs
}

type georadiusOrderDesc struct {
	cs []string
}

func (c georadiusOrderDesc) Store(key string) georadiusStore {
	return georadiusStore{cs: append(c.cs, "STORE", key)}
}

func (c georadiusOrderDesc) Storedist(key string) georadiusStoredist {
	return georadiusStoredist{cs: append(c.cs, "STOREDIST", key)}
}

func (c georadiusOrderDesc) Build() []string {
	return c.cs
}

type georadiusRadius struct {
	cs []string
}

func (c georadiusRadius) M() georadiusUnitM {
	return georadiusUnitM{cs: append(c.cs, "m")}
}

func (c georadiusRadius) Km() georadiusUnitKm {
	return georadiusUnitKm{cs: append(c.cs, "km")}
}

func (c georadiusRadius) Ft() georadiusUnitFt {
	return georadiusUnitFt{cs: append(c.cs, "ft")}
}

func (c georadiusRadius) Mi() georadiusUnitMi {
	return georadiusUnitMi{cs: append(c.cs, "mi")}
}

type georadiusStore struct {
	cs []string
}

func (c georadiusStore) Storedist(key string) georadiusStoredist {
	return georadiusStoredist{cs: append(c.cs, "STOREDIST", key)}
}

func (c georadiusStore) Build() []string {
	return c.cs
}

type georadiusStoredist struct {
	cs []string
}

func (c georadiusStoredist) Build() []string {
	return c.cs
}

type georadiusUnitFt struct {
	cs []string
}

func (c georadiusUnitFt) Withcoord() georadiusWithcoordWithcoord {
	return georadiusWithcoordWithcoord{cs: append(c.cs, "WITHCOORD")}
}

func (c georadiusUnitFt) Withdist() georadiusWithdistWithdist {
	return georadiusWithdistWithdist{cs: append(c.cs, "WITHDIST")}
}

func (c georadiusUnitFt) Withhash() georadiusWithhashWithhash {
	return georadiusWithhashWithhash{cs: append(c.cs, "WITHHASH")}
}

func (c georadiusUnitFt) Count(count int64) georadiusCountCount {
	return georadiusCountCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c georadiusUnitFt) Asc() georadiusOrderAsc {
	return georadiusOrderAsc{cs: append(c.cs, "ASC")}
}

func (c georadiusUnitFt) Desc() georadiusOrderDesc {
	return georadiusOrderDesc{cs: append(c.cs, "DESC")}
}

func (c georadiusUnitFt) Store(key string) georadiusStore {
	return georadiusStore{cs: append(c.cs, "STORE", key)}
}

func (c georadiusUnitFt) Storedist(key string) georadiusStoredist {
	return georadiusStoredist{cs: append(c.cs, "STOREDIST", key)}
}

type georadiusUnitKm struct {
	cs []string
}

func (c georadiusUnitKm) Withcoord() georadiusWithcoordWithcoord {
	return georadiusWithcoordWithcoord{cs: append(c.cs, "WITHCOORD")}
}

func (c georadiusUnitKm) Withdist() georadiusWithdistWithdist {
	return georadiusWithdistWithdist{cs: append(c.cs, "WITHDIST")}
}

func (c georadiusUnitKm) Withhash() georadiusWithhashWithhash {
	return georadiusWithhashWithhash{cs: append(c.cs, "WITHHASH")}
}

func (c georadiusUnitKm) Count(count int64) georadiusCountCount {
	return georadiusCountCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c georadiusUnitKm) Asc() georadiusOrderAsc {
	return georadiusOrderAsc{cs: append(c.cs, "ASC")}
}

func (c georadiusUnitKm) Desc() georadiusOrderDesc {
	return georadiusOrderDesc{cs: append(c.cs, "DESC")}
}

func (c georadiusUnitKm) Store(key string) georadiusStore {
	return georadiusStore{cs: append(c.cs, "STORE", key)}
}

func (c georadiusUnitKm) Storedist(key string) georadiusStoredist {
	return georadiusStoredist{cs: append(c.cs, "STOREDIST", key)}
}

type georadiusUnitM struct {
	cs []string
}

func (c georadiusUnitM) Withcoord() georadiusWithcoordWithcoord {
	return georadiusWithcoordWithcoord{cs: append(c.cs, "WITHCOORD")}
}

func (c georadiusUnitM) Withdist() georadiusWithdistWithdist {
	return georadiusWithdistWithdist{cs: append(c.cs, "WITHDIST")}
}

func (c georadiusUnitM) Withhash() georadiusWithhashWithhash {
	return georadiusWithhashWithhash{cs: append(c.cs, "WITHHASH")}
}

func (c georadiusUnitM) Count(count int64) georadiusCountCount {
	return georadiusCountCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c georadiusUnitM) Asc() georadiusOrderAsc {
	return georadiusOrderAsc{cs: append(c.cs, "ASC")}
}

func (c georadiusUnitM) Desc() georadiusOrderDesc {
	return georadiusOrderDesc{cs: append(c.cs, "DESC")}
}

func (c georadiusUnitM) Store(key string) georadiusStore {
	return georadiusStore{cs: append(c.cs, "STORE", key)}
}

func (c georadiusUnitM) Storedist(key string) georadiusStoredist {
	return georadiusStoredist{cs: append(c.cs, "STOREDIST", key)}
}

type georadiusUnitMi struct {
	cs []string
}

func (c georadiusUnitMi) Withcoord() georadiusWithcoordWithcoord {
	return georadiusWithcoordWithcoord{cs: append(c.cs, "WITHCOORD")}
}

func (c georadiusUnitMi) Withdist() georadiusWithdistWithdist {
	return georadiusWithdistWithdist{cs: append(c.cs, "WITHDIST")}
}

func (c georadiusUnitMi) Withhash() georadiusWithhashWithhash {
	return georadiusWithhashWithhash{cs: append(c.cs, "WITHHASH")}
}

func (c georadiusUnitMi) Count(count int64) georadiusCountCount {
	return georadiusCountCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c georadiusUnitMi) Asc() georadiusOrderAsc {
	return georadiusOrderAsc{cs: append(c.cs, "ASC")}
}

func (c georadiusUnitMi) Desc() georadiusOrderDesc {
	return georadiusOrderDesc{cs: append(c.cs, "DESC")}
}

func (c georadiusUnitMi) Store(key string) georadiusStore {
	return georadiusStore{cs: append(c.cs, "STORE", key)}
}

func (c georadiusUnitMi) Storedist(key string) georadiusStoredist {
	return georadiusStoredist{cs: append(c.cs, "STOREDIST", key)}
}

type georadiusWithcoordWithcoord struct {
	cs []string
}

func (c georadiusWithcoordWithcoord) Withdist() georadiusWithdistWithdist {
	return georadiusWithdistWithdist{cs: append(c.cs, "WITHDIST")}
}

func (c georadiusWithcoordWithcoord) Withhash() georadiusWithhashWithhash {
	return georadiusWithhashWithhash{cs: append(c.cs, "WITHHASH")}
}

func (c georadiusWithcoordWithcoord) Count(count int64) georadiusCountCount {
	return georadiusCountCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c georadiusWithcoordWithcoord) Asc() georadiusOrderAsc {
	return georadiusOrderAsc{cs: append(c.cs, "ASC")}
}

func (c georadiusWithcoordWithcoord) Desc() georadiusOrderDesc {
	return georadiusOrderDesc{cs: append(c.cs, "DESC")}
}

func (c georadiusWithcoordWithcoord) Store(key string) georadiusStore {
	return georadiusStore{cs: append(c.cs, "STORE", key)}
}

func (c georadiusWithcoordWithcoord) Storedist(key string) georadiusStoredist {
	return georadiusStoredist{cs: append(c.cs, "STOREDIST", key)}
}

type georadiusWithdistWithdist struct {
	cs []string
}

func (c georadiusWithdistWithdist) Withhash() georadiusWithhashWithhash {
	return georadiusWithhashWithhash{cs: append(c.cs, "WITHHASH")}
}

func (c georadiusWithdistWithdist) Count(count int64) georadiusCountCount {
	return georadiusCountCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c georadiusWithdistWithdist) Asc() georadiusOrderAsc {
	return georadiusOrderAsc{cs: append(c.cs, "ASC")}
}

func (c georadiusWithdistWithdist) Desc() georadiusOrderDesc {
	return georadiusOrderDesc{cs: append(c.cs, "DESC")}
}

func (c georadiusWithdistWithdist) Store(key string) georadiusStore {
	return georadiusStore{cs: append(c.cs, "STORE", key)}
}

func (c georadiusWithdistWithdist) Storedist(key string) georadiusStoredist {
	return georadiusStoredist{cs: append(c.cs, "STOREDIST", key)}
}

type georadiusWithhashWithhash struct {
	cs []string
}

func (c georadiusWithhashWithhash) Count(count int64) georadiusCountCount {
	return georadiusCountCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c georadiusWithhashWithhash) Asc() georadiusOrderAsc {
	return georadiusOrderAsc{cs: append(c.cs, "ASC")}
}

func (c georadiusWithhashWithhash) Desc() georadiusOrderDesc {
	return georadiusOrderDesc{cs: append(c.cs, "DESC")}
}

func (c georadiusWithhashWithhash) Store(key string) georadiusStore {
	return georadiusStore{cs: append(c.cs, "STORE", key)}
}

func (c georadiusWithhashWithhash) Storedist(key string) georadiusStoredist {
	return georadiusStoredist{cs: append(c.cs, "STOREDIST", key)}
}

type georadiusbymember struct {
	cs []string
}

func (c georadiusbymember) Key(key string) georadiusbymemberKey {
	return georadiusbymemberKey{cs: append(c.cs, key)}
}

func (b *Builder) Georadiusbymember() (c georadiusbymember) {
	c.cs = append(b.get(), "GEORADIUSBYMEMBER")
	return
}

type georadiusbymemberCountAnyAny struct {
	cs []string
}

func (c georadiusbymemberCountAnyAny) Asc() georadiusbymemberOrderAsc {
	return georadiusbymemberOrderAsc{cs: append(c.cs, "ASC")}
}

func (c georadiusbymemberCountAnyAny) Desc() georadiusbymemberOrderDesc {
	return georadiusbymemberOrderDesc{cs: append(c.cs, "DESC")}
}

func (c georadiusbymemberCountAnyAny) Store(key string) georadiusbymemberStore {
	return georadiusbymemberStore{cs: append(c.cs, "STORE", key)}
}

func (c georadiusbymemberCountAnyAny) Storedist(key string) georadiusbymemberStoredist {
	return georadiusbymemberStoredist{cs: append(c.cs, "STOREDIST", key)}
}

func (c georadiusbymemberCountAnyAny) Build() []string {
	return c.cs
}

type georadiusbymemberCountCount struct {
	cs []string
}

func (c georadiusbymemberCountCount) Any() georadiusbymemberCountAnyAny {
	return georadiusbymemberCountAnyAny{cs: append(c.cs, "ANY")}
}

func (c georadiusbymemberCountCount) Asc() georadiusbymemberOrderAsc {
	return georadiusbymemberOrderAsc{cs: append(c.cs, "ASC")}
}

func (c georadiusbymemberCountCount) Desc() georadiusbymemberOrderDesc {
	return georadiusbymemberOrderDesc{cs: append(c.cs, "DESC")}
}

func (c georadiusbymemberCountCount) Store(key string) georadiusbymemberStore {
	return georadiusbymemberStore{cs: append(c.cs, "STORE", key)}
}

func (c georadiusbymemberCountCount) Storedist(key string) georadiusbymemberStoredist {
	return georadiusbymemberStoredist{cs: append(c.cs, "STOREDIST", key)}
}

func (c georadiusbymemberCountCount) Build() []string {
	return c.cs
}

type georadiusbymemberKey struct {
	cs []string
}

func (c georadiusbymemberKey) Member(member string) georadiusbymemberMember {
	return georadiusbymemberMember{cs: append(c.cs, member)}
}

type georadiusbymemberMember struct {
	cs []string
}

func (c georadiusbymemberMember) Radius(radius float64) georadiusbymemberRadius {
	return georadiusbymemberRadius{cs: append(c.cs, strconv.FormatFloat(radius, 'f', -1, 64))}
}

type georadiusbymemberOrderAsc struct {
	cs []string
}

func (c georadiusbymemberOrderAsc) Store(key string) georadiusbymemberStore {
	return georadiusbymemberStore{cs: append(c.cs, "STORE", key)}
}

func (c georadiusbymemberOrderAsc) Storedist(key string) georadiusbymemberStoredist {
	return georadiusbymemberStoredist{cs: append(c.cs, "STOREDIST", key)}
}

func (c georadiusbymemberOrderAsc) Build() []string {
	return c.cs
}

type georadiusbymemberOrderDesc struct {
	cs []string
}

func (c georadiusbymemberOrderDesc) Store(key string) georadiusbymemberStore {
	return georadiusbymemberStore{cs: append(c.cs, "STORE", key)}
}

func (c georadiusbymemberOrderDesc) Storedist(key string) georadiusbymemberStoredist {
	return georadiusbymemberStoredist{cs: append(c.cs, "STOREDIST", key)}
}

func (c georadiusbymemberOrderDesc) Build() []string {
	return c.cs
}

type georadiusbymemberRadius struct {
	cs []string
}

func (c georadiusbymemberRadius) M() georadiusbymemberUnitM {
	return georadiusbymemberUnitM{cs: append(c.cs, "m")}
}

func (c georadiusbymemberRadius) Km() georadiusbymemberUnitKm {
	return georadiusbymemberUnitKm{cs: append(c.cs, "km")}
}

func (c georadiusbymemberRadius) Ft() georadiusbymemberUnitFt {
	return georadiusbymemberUnitFt{cs: append(c.cs, "ft")}
}

func (c georadiusbymemberRadius) Mi() georadiusbymemberUnitMi {
	return georadiusbymemberUnitMi{cs: append(c.cs, "mi")}
}

type georadiusbymemberStore struct {
	cs []string
}

func (c georadiusbymemberStore) Storedist(key string) georadiusbymemberStoredist {
	return georadiusbymemberStoredist{cs: append(c.cs, "STOREDIST", key)}
}

func (c georadiusbymemberStore) Build() []string {
	return c.cs
}

type georadiusbymemberStoredist struct {
	cs []string
}

func (c georadiusbymemberStoredist) Build() []string {
	return c.cs
}

type georadiusbymemberUnitFt struct {
	cs []string
}

func (c georadiusbymemberUnitFt) Withcoord() georadiusbymemberWithcoordWithcoord {
	return georadiusbymemberWithcoordWithcoord{cs: append(c.cs, "WITHCOORD")}
}

func (c georadiusbymemberUnitFt) Withdist() georadiusbymemberWithdistWithdist {
	return georadiusbymemberWithdistWithdist{cs: append(c.cs, "WITHDIST")}
}

func (c georadiusbymemberUnitFt) Withhash() georadiusbymemberWithhashWithhash {
	return georadiusbymemberWithhashWithhash{cs: append(c.cs, "WITHHASH")}
}

func (c georadiusbymemberUnitFt) Count(count int64) georadiusbymemberCountCount {
	return georadiusbymemberCountCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c georadiusbymemberUnitFt) Asc() georadiusbymemberOrderAsc {
	return georadiusbymemberOrderAsc{cs: append(c.cs, "ASC")}
}

func (c georadiusbymemberUnitFt) Desc() georadiusbymemberOrderDesc {
	return georadiusbymemberOrderDesc{cs: append(c.cs, "DESC")}
}

func (c georadiusbymemberUnitFt) Store(key string) georadiusbymemberStore {
	return georadiusbymemberStore{cs: append(c.cs, "STORE", key)}
}

func (c georadiusbymemberUnitFt) Storedist(key string) georadiusbymemberStoredist {
	return georadiusbymemberStoredist{cs: append(c.cs, "STOREDIST", key)}
}

type georadiusbymemberUnitKm struct {
	cs []string
}

func (c georadiusbymemberUnitKm) Withcoord() georadiusbymemberWithcoordWithcoord {
	return georadiusbymemberWithcoordWithcoord{cs: append(c.cs, "WITHCOORD")}
}

func (c georadiusbymemberUnitKm) Withdist() georadiusbymemberWithdistWithdist {
	return georadiusbymemberWithdistWithdist{cs: append(c.cs, "WITHDIST")}
}

func (c georadiusbymemberUnitKm) Withhash() georadiusbymemberWithhashWithhash {
	return georadiusbymemberWithhashWithhash{cs: append(c.cs, "WITHHASH")}
}

func (c georadiusbymemberUnitKm) Count(count int64) georadiusbymemberCountCount {
	return georadiusbymemberCountCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c georadiusbymemberUnitKm) Asc() georadiusbymemberOrderAsc {
	return georadiusbymemberOrderAsc{cs: append(c.cs, "ASC")}
}

func (c georadiusbymemberUnitKm) Desc() georadiusbymemberOrderDesc {
	return georadiusbymemberOrderDesc{cs: append(c.cs, "DESC")}
}

func (c georadiusbymemberUnitKm) Store(key string) georadiusbymemberStore {
	return georadiusbymemberStore{cs: append(c.cs, "STORE", key)}
}

func (c georadiusbymemberUnitKm) Storedist(key string) georadiusbymemberStoredist {
	return georadiusbymemberStoredist{cs: append(c.cs, "STOREDIST", key)}
}

type georadiusbymemberUnitM struct {
	cs []string
}

func (c georadiusbymemberUnitM) Withcoord() georadiusbymemberWithcoordWithcoord {
	return georadiusbymemberWithcoordWithcoord{cs: append(c.cs, "WITHCOORD")}
}

func (c georadiusbymemberUnitM) Withdist() georadiusbymemberWithdistWithdist {
	return georadiusbymemberWithdistWithdist{cs: append(c.cs, "WITHDIST")}
}

func (c georadiusbymemberUnitM) Withhash() georadiusbymemberWithhashWithhash {
	return georadiusbymemberWithhashWithhash{cs: append(c.cs, "WITHHASH")}
}

func (c georadiusbymemberUnitM) Count(count int64) georadiusbymemberCountCount {
	return georadiusbymemberCountCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c georadiusbymemberUnitM) Asc() georadiusbymemberOrderAsc {
	return georadiusbymemberOrderAsc{cs: append(c.cs, "ASC")}
}

func (c georadiusbymemberUnitM) Desc() georadiusbymemberOrderDesc {
	return georadiusbymemberOrderDesc{cs: append(c.cs, "DESC")}
}

func (c georadiusbymemberUnitM) Store(key string) georadiusbymemberStore {
	return georadiusbymemberStore{cs: append(c.cs, "STORE", key)}
}

func (c georadiusbymemberUnitM) Storedist(key string) georadiusbymemberStoredist {
	return georadiusbymemberStoredist{cs: append(c.cs, "STOREDIST", key)}
}

type georadiusbymemberUnitMi struct {
	cs []string
}

func (c georadiusbymemberUnitMi) Withcoord() georadiusbymemberWithcoordWithcoord {
	return georadiusbymemberWithcoordWithcoord{cs: append(c.cs, "WITHCOORD")}
}

func (c georadiusbymemberUnitMi) Withdist() georadiusbymemberWithdistWithdist {
	return georadiusbymemberWithdistWithdist{cs: append(c.cs, "WITHDIST")}
}

func (c georadiusbymemberUnitMi) Withhash() georadiusbymemberWithhashWithhash {
	return georadiusbymemberWithhashWithhash{cs: append(c.cs, "WITHHASH")}
}

func (c georadiusbymemberUnitMi) Count(count int64) georadiusbymemberCountCount {
	return georadiusbymemberCountCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c georadiusbymemberUnitMi) Asc() georadiusbymemberOrderAsc {
	return georadiusbymemberOrderAsc{cs: append(c.cs, "ASC")}
}

func (c georadiusbymemberUnitMi) Desc() georadiusbymemberOrderDesc {
	return georadiusbymemberOrderDesc{cs: append(c.cs, "DESC")}
}

func (c georadiusbymemberUnitMi) Store(key string) georadiusbymemberStore {
	return georadiusbymemberStore{cs: append(c.cs, "STORE", key)}
}

func (c georadiusbymemberUnitMi) Storedist(key string) georadiusbymemberStoredist {
	return georadiusbymemberStoredist{cs: append(c.cs, "STOREDIST", key)}
}

type georadiusbymemberWithcoordWithcoord struct {
	cs []string
}

func (c georadiusbymemberWithcoordWithcoord) Withdist() georadiusbymemberWithdistWithdist {
	return georadiusbymemberWithdistWithdist{cs: append(c.cs, "WITHDIST")}
}

func (c georadiusbymemberWithcoordWithcoord) Withhash() georadiusbymemberWithhashWithhash {
	return georadiusbymemberWithhashWithhash{cs: append(c.cs, "WITHHASH")}
}

func (c georadiusbymemberWithcoordWithcoord) Count(count int64) georadiusbymemberCountCount {
	return georadiusbymemberCountCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c georadiusbymemberWithcoordWithcoord) Asc() georadiusbymemberOrderAsc {
	return georadiusbymemberOrderAsc{cs: append(c.cs, "ASC")}
}

func (c georadiusbymemberWithcoordWithcoord) Desc() georadiusbymemberOrderDesc {
	return georadiusbymemberOrderDesc{cs: append(c.cs, "DESC")}
}

func (c georadiusbymemberWithcoordWithcoord) Store(key string) georadiusbymemberStore {
	return georadiusbymemberStore{cs: append(c.cs, "STORE", key)}
}

func (c georadiusbymemberWithcoordWithcoord) Storedist(key string) georadiusbymemberStoredist {
	return georadiusbymemberStoredist{cs: append(c.cs, "STOREDIST", key)}
}

type georadiusbymemberWithdistWithdist struct {
	cs []string
}

func (c georadiusbymemberWithdistWithdist) Withhash() georadiusbymemberWithhashWithhash {
	return georadiusbymemberWithhashWithhash{cs: append(c.cs, "WITHHASH")}
}

func (c georadiusbymemberWithdistWithdist) Count(count int64) georadiusbymemberCountCount {
	return georadiusbymemberCountCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c georadiusbymemberWithdistWithdist) Asc() georadiusbymemberOrderAsc {
	return georadiusbymemberOrderAsc{cs: append(c.cs, "ASC")}
}

func (c georadiusbymemberWithdistWithdist) Desc() georadiusbymemberOrderDesc {
	return georadiusbymemberOrderDesc{cs: append(c.cs, "DESC")}
}

func (c georadiusbymemberWithdistWithdist) Store(key string) georadiusbymemberStore {
	return georadiusbymemberStore{cs: append(c.cs, "STORE", key)}
}

func (c georadiusbymemberWithdistWithdist) Storedist(key string) georadiusbymemberStoredist {
	return georadiusbymemberStoredist{cs: append(c.cs, "STOREDIST", key)}
}

type georadiusbymemberWithhashWithhash struct {
	cs []string
}

func (c georadiusbymemberWithhashWithhash) Count(count int64) georadiusbymemberCountCount {
	return georadiusbymemberCountCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c georadiusbymemberWithhashWithhash) Asc() georadiusbymemberOrderAsc {
	return georadiusbymemberOrderAsc{cs: append(c.cs, "ASC")}
}

func (c georadiusbymemberWithhashWithhash) Desc() georadiusbymemberOrderDesc {
	return georadiusbymemberOrderDesc{cs: append(c.cs, "DESC")}
}

func (c georadiusbymemberWithhashWithhash) Store(key string) georadiusbymemberStore {
	return georadiusbymemberStore{cs: append(c.cs, "STORE", key)}
}

func (c georadiusbymemberWithhashWithhash) Storedist(key string) georadiusbymemberStoredist {
	return georadiusbymemberStoredist{cs: append(c.cs, "STOREDIST", key)}
}

type geosearch struct {
	cs []string
}

func (c geosearch) Key(key string) geosearchKey {
	return geosearchKey{cs: append(c.cs, key)}
}

func (b *Builder) Geosearch() (c geosearch) {
	c.cs = append(b.get(), "GEOSEARCH")
	return
}

type geosearchBoxBybox struct {
	cs []string
}

func (c geosearchBoxBybox) Height(height float64) geosearchBoxHeight {
	return geosearchBoxHeight{cs: append(c.cs, strconv.FormatFloat(height, 'f', -1, 64))}
}

type geosearchBoxHeight struct {
	cs []string
}

func (c geosearchBoxHeight) M() geosearchBoxUnitM {
	return geosearchBoxUnitM{cs: append(c.cs, "m")}
}

func (c geosearchBoxHeight) Km() geosearchBoxUnitKm {
	return geosearchBoxUnitKm{cs: append(c.cs, "km")}
}

func (c geosearchBoxHeight) Ft() geosearchBoxUnitFt {
	return geosearchBoxUnitFt{cs: append(c.cs, "ft")}
}

func (c geosearchBoxHeight) Mi() geosearchBoxUnitMi {
	return geosearchBoxUnitMi{cs: append(c.cs, "mi")}
}

type geosearchBoxUnitFt struct {
	cs []string
}

func (c geosearchBoxUnitFt) Asc() geosearchOrderAsc {
	return geosearchOrderAsc{cs: append(c.cs, "ASC")}
}

func (c geosearchBoxUnitFt) Desc() geosearchOrderDesc {
	return geosearchOrderDesc{cs: append(c.cs, "DESC")}
}

func (c geosearchBoxUnitFt) Count(count int64) geosearchCountCount {
	return geosearchCountCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c geosearchBoxUnitFt) Withcoord() geosearchWithcoordWithcoord {
	return geosearchWithcoordWithcoord{cs: append(c.cs, "WITHCOORD")}
}

func (c geosearchBoxUnitFt) Withdist() geosearchWithdistWithdist {
	return geosearchWithdistWithdist{cs: append(c.cs, "WITHDIST")}
}

func (c geosearchBoxUnitFt) Withhash() geosearchWithhashWithhash {
	return geosearchWithhashWithhash{cs: append(c.cs, "WITHHASH")}
}

type geosearchBoxUnitKm struct {
	cs []string
}

func (c geosearchBoxUnitKm) Asc() geosearchOrderAsc {
	return geosearchOrderAsc{cs: append(c.cs, "ASC")}
}

func (c geosearchBoxUnitKm) Desc() geosearchOrderDesc {
	return geosearchOrderDesc{cs: append(c.cs, "DESC")}
}

func (c geosearchBoxUnitKm) Count(count int64) geosearchCountCount {
	return geosearchCountCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c geosearchBoxUnitKm) Withcoord() geosearchWithcoordWithcoord {
	return geosearchWithcoordWithcoord{cs: append(c.cs, "WITHCOORD")}
}

func (c geosearchBoxUnitKm) Withdist() geosearchWithdistWithdist {
	return geosearchWithdistWithdist{cs: append(c.cs, "WITHDIST")}
}

func (c geosearchBoxUnitKm) Withhash() geosearchWithhashWithhash {
	return geosearchWithhashWithhash{cs: append(c.cs, "WITHHASH")}
}

type geosearchBoxUnitM struct {
	cs []string
}

func (c geosearchBoxUnitM) Asc() geosearchOrderAsc {
	return geosearchOrderAsc{cs: append(c.cs, "ASC")}
}

func (c geosearchBoxUnitM) Desc() geosearchOrderDesc {
	return geosearchOrderDesc{cs: append(c.cs, "DESC")}
}

func (c geosearchBoxUnitM) Count(count int64) geosearchCountCount {
	return geosearchCountCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c geosearchBoxUnitM) Withcoord() geosearchWithcoordWithcoord {
	return geosearchWithcoordWithcoord{cs: append(c.cs, "WITHCOORD")}
}

func (c geosearchBoxUnitM) Withdist() geosearchWithdistWithdist {
	return geosearchWithdistWithdist{cs: append(c.cs, "WITHDIST")}
}

func (c geosearchBoxUnitM) Withhash() geosearchWithhashWithhash {
	return geosearchWithhashWithhash{cs: append(c.cs, "WITHHASH")}
}

type geosearchBoxUnitMi struct {
	cs []string
}

func (c geosearchBoxUnitMi) Asc() geosearchOrderAsc {
	return geosearchOrderAsc{cs: append(c.cs, "ASC")}
}

func (c geosearchBoxUnitMi) Desc() geosearchOrderDesc {
	return geosearchOrderDesc{cs: append(c.cs, "DESC")}
}

func (c geosearchBoxUnitMi) Count(count int64) geosearchCountCount {
	return geosearchCountCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c geosearchBoxUnitMi) Withcoord() geosearchWithcoordWithcoord {
	return geosearchWithcoordWithcoord{cs: append(c.cs, "WITHCOORD")}
}

func (c geosearchBoxUnitMi) Withdist() geosearchWithdistWithdist {
	return geosearchWithdistWithdist{cs: append(c.cs, "WITHDIST")}
}

func (c geosearchBoxUnitMi) Withhash() geosearchWithhashWithhash {
	return geosearchWithhashWithhash{cs: append(c.cs, "WITHHASH")}
}

type geosearchCircleByradius struct {
	cs []string
}

func (c geosearchCircleByradius) M() geosearchCircleUnitM {
	return geosearchCircleUnitM{cs: append(c.cs, "m")}
}

func (c geosearchCircleByradius) Km() geosearchCircleUnitKm {
	return geosearchCircleUnitKm{cs: append(c.cs, "km")}
}

func (c geosearchCircleByradius) Ft() geosearchCircleUnitFt {
	return geosearchCircleUnitFt{cs: append(c.cs, "ft")}
}

func (c geosearchCircleByradius) Mi() geosearchCircleUnitMi {
	return geosearchCircleUnitMi{cs: append(c.cs, "mi")}
}

type geosearchCircleUnitFt struct {
	cs []string
}

func (c geosearchCircleUnitFt) Bybox(width float64) geosearchBoxBybox {
	return geosearchBoxBybox{cs: append(c.cs, "BYBOX", strconv.FormatFloat(width, 'f', -1, 64))}
}

func (c geosearchCircleUnitFt) Asc() geosearchOrderAsc {
	return geosearchOrderAsc{cs: append(c.cs, "ASC")}
}

func (c geosearchCircleUnitFt) Desc() geosearchOrderDesc {
	return geosearchOrderDesc{cs: append(c.cs, "DESC")}
}

func (c geosearchCircleUnitFt) Count(count int64) geosearchCountCount {
	return geosearchCountCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c geosearchCircleUnitFt) Withcoord() geosearchWithcoordWithcoord {
	return geosearchWithcoordWithcoord{cs: append(c.cs, "WITHCOORD")}
}

func (c geosearchCircleUnitFt) Withdist() geosearchWithdistWithdist {
	return geosearchWithdistWithdist{cs: append(c.cs, "WITHDIST")}
}

func (c geosearchCircleUnitFt) Withhash() geosearchWithhashWithhash {
	return geosearchWithhashWithhash{cs: append(c.cs, "WITHHASH")}
}

type geosearchCircleUnitKm struct {
	cs []string
}

func (c geosearchCircleUnitKm) Bybox(width float64) geosearchBoxBybox {
	return geosearchBoxBybox{cs: append(c.cs, "BYBOX", strconv.FormatFloat(width, 'f', -1, 64))}
}

func (c geosearchCircleUnitKm) Asc() geosearchOrderAsc {
	return geosearchOrderAsc{cs: append(c.cs, "ASC")}
}

func (c geosearchCircleUnitKm) Desc() geosearchOrderDesc {
	return geosearchOrderDesc{cs: append(c.cs, "DESC")}
}

func (c geosearchCircleUnitKm) Count(count int64) geosearchCountCount {
	return geosearchCountCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c geosearchCircleUnitKm) Withcoord() geosearchWithcoordWithcoord {
	return geosearchWithcoordWithcoord{cs: append(c.cs, "WITHCOORD")}
}

func (c geosearchCircleUnitKm) Withdist() geosearchWithdistWithdist {
	return geosearchWithdistWithdist{cs: append(c.cs, "WITHDIST")}
}

func (c geosearchCircleUnitKm) Withhash() geosearchWithhashWithhash {
	return geosearchWithhashWithhash{cs: append(c.cs, "WITHHASH")}
}

type geosearchCircleUnitM struct {
	cs []string
}

func (c geosearchCircleUnitM) Bybox(width float64) geosearchBoxBybox {
	return geosearchBoxBybox{cs: append(c.cs, "BYBOX", strconv.FormatFloat(width, 'f', -1, 64))}
}

func (c geosearchCircleUnitM) Asc() geosearchOrderAsc {
	return geosearchOrderAsc{cs: append(c.cs, "ASC")}
}

func (c geosearchCircleUnitM) Desc() geosearchOrderDesc {
	return geosearchOrderDesc{cs: append(c.cs, "DESC")}
}

func (c geosearchCircleUnitM) Count(count int64) geosearchCountCount {
	return geosearchCountCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c geosearchCircleUnitM) Withcoord() geosearchWithcoordWithcoord {
	return geosearchWithcoordWithcoord{cs: append(c.cs, "WITHCOORD")}
}

func (c geosearchCircleUnitM) Withdist() geosearchWithdistWithdist {
	return geosearchWithdistWithdist{cs: append(c.cs, "WITHDIST")}
}

func (c geosearchCircleUnitM) Withhash() geosearchWithhashWithhash {
	return geosearchWithhashWithhash{cs: append(c.cs, "WITHHASH")}
}

type geosearchCircleUnitMi struct {
	cs []string
}

func (c geosearchCircleUnitMi) Bybox(width float64) geosearchBoxBybox {
	return geosearchBoxBybox{cs: append(c.cs, "BYBOX", strconv.FormatFloat(width, 'f', -1, 64))}
}

func (c geosearchCircleUnitMi) Asc() geosearchOrderAsc {
	return geosearchOrderAsc{cs: append(c.cs, "ASC")}
}

func (c geosearchCircleUnitMi) Desc() geosearchOrderDesc {
	return geosearchOrderDesc{cs: append(c.cs, "DESC")}
}

func (c geosearchCircleUnitMi) Count(count int64) geosearchCountCount {
	return geosearchCountCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c geosearchCircleUnitMi) Withcoord() geosearchWithcoordWithcoord {
	return geosearchWithcoordWithcoord{cs: append(c.cs, "WITHCOORD")}
}

func (c geosearchCircleUnitMi) Withdist() geosearchWithdistWithdist {
	return geosearchWithdistWithdist{cs: append(c.cs, "WITHDIST")}
}

func (c geosearchCircleUnitMi) Withhash() geosearchWithhashWithhash {
	return geosearchWithhashWithhash{cs: append(c.cs, "WITHHASH")}
}

type geosearchCountAnyAny struct {
	cs []string
}

func (c geosearchCountAnyAny) Withcoord() geosearchWithcoordWithcoord {
	return geosearchWithcoordWithcoord{cs: append(c.cs, "WITHCOORD")}
}

func (c geosearchCountAnyAny) Withdist() geosearchWithdistWithdist {
	return geosearchWithdistWithdist{cs: append(c.cs, "WITHDIST")}
}

func (c geosearchCountAnyAny) Withhash() geosearchWithhashWithhash {
	return geosearchWithhashWithhash{cs: append(c.cs, "WITHHASH")}
}

func (c geosearchCountAnyAny) Build() []string {
	return c.cs
}

type geosearchCountCount struct {
	cs []string
}

func (c geosearchCountCount) Any() geosearchCountAnyAny {
	return geosearchCountAnyAny{cs: append(c.cs, "ANY")}
}

func (c geosearchCountCount) Withcoord() geosearchWithcoordWithcoord {
	return geosearchWithcoordWithcoord{cs: append(c.cs, "WITHCOORD")}
}

func (c geosearchCountCount) Withdist() geosearchWithdistWithdist {
	return geosearchWithdistWithdist{cs: append(c.cs, "WITHDIST")}
}

func (c geosearchCountCount) Withhash() geosearchWithhashWithhash {
	return geosearchWithhashWithhash{cs: append(c.cs, "WITHHASH")}
}

func (c geosearchCountCount) Build() []string {
	return c.cs
}

type geosearchFromlonlat struct {
	cs []string
}

func (c geosearchFromlonlat) Byradius(radius float64) geosearchCircleByradius {
	return geosearchCircleByradius{cs: append(c.cs, "BYRADIUS", strconv.FormatFloat(radius, 'f', -1, 64))}
}

func (c geosearchFromlonlat) Bybox(width float64) geosearchBoxBybox {
	return geosearchBoxBybox{cs: append(c.cs, "BYBOX", strconv.FormatFloat(width, 'f', -1, 64))}
}

func (c geosearchFromlonlat) Asc() geosearchOrderAsc {
	return geosearchOrderAsc{cs: append(c.cs, "ASC")}
}

func (c geosearchFromlonlat) Desc() geosearchOrderDesc {
	return geosearchOrderDesc{cs: append(c.cs, "DESC")}
}

func (c geosearchFromlonlat) Count(count int64) geosearchCountCount {
	return geosearchCountCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c geosearchFromlonlat) Withcoord() geosearchWithcoordWithcoord {
	return geosearchWithcoordWithcoord{cs: append(c.cs, "WITHCOORD")}
}

func (c geosearchFromlonlat) Withdist() geosearchWithdistWithdist {
	return geosearchWithdistWithdist{cs: append(c.cs, "WITHDIST")}
}

func (c geosearchFromlonlat) Withhash() geosearchWithhashWithhash {
	return geosearchWithhashWithhash{cs: append(c.cs, "WITHHASH")}
}

type geosearchFrommember struct {
	cs []string
}

func (c geosearchFrommember) Fromlonlat(longitude float64, latitude float64) geosearchFromlonlat {
	return geosearchFromlonlat{cs: append(c.cs, "FROMLONLAT", strconv.FormatFloat(longitude, 'f', -1, 64), strconv.FormatFloat(latitude, 'f', -1, 64))}
}

func (c geosearchFrommember) Byradius(radius float64) geosearchCircleByradius {
	return geosearchCircleByradius{cs: append(c.cs, "BYRADIUS", strconv.FormatFloat(radius, 'f', -1, 64))}
}

func (c geosearchFrommember) Bybox(width float64) geosearchBoxBybox {
	return geosearchBoxBybox{cs: append(c.cs, "BYBOX", strconv.FormatFloat(width, 'f', -1, 64))}
}

func (c geosearchFrommember) Asc() geosearchOrderAsc {
	return geosearchOrderAsc{cs: append(c.cs, "ASC")}
}

func (c geosearchFrommember) Desc() geosearchOrderDesc {
	return geosearchOrderDesc{cs: append(c.cs, "DESC")}
}

func (c geosearchFrommember) Count(count int64) geosearchCountCount {
	return geosearchCountCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c geosearchFrommember) Withcoord() geosearchWithcoordWithcoord {
	return geosearchWithcoordWithcoord{cs: append(c.cs, "WITHCOORD")}
}

func (c geosearchFrommember) Withdist() geosearchWithdistWithdist {
	return geosearchWithdistWithdist{cs: append(c.cs, "WITHDIST")}
}

func (c geosearchFrommember) Withhash() geosearchWithhashWithhash {
	return geosearchWithhashWithhash{cs: append(c.cs, "WITHHASH")}
}

type geosearchKey struct {
	cs []string
}

func (c geosearchKey) Frommember(member string) geosearchFrommember {
	return geosearchFrommember{cs: append(c.cs, "FROMMEMBER", member)}
}

func (c geosearchKey) Fromlonlat(longitude float64, latitude float64) geosearchFromlonlat {
	return geosearchFromlonlat{cs: append(c.cs, "FROMLONLAT", strconv.FormatFloat(longitude, 'f', -1, 64), strconv.FormatFloat(latitude, 'f', -1, 64))}
}

func (c geosearchKey) Byradius(radius float64) geosearchCircleByradius {
	return geosearchCircleByradius{cs: append(c.cs, "BYRADIUS", strconv.FormatFloat(radius, 'f', -1, 64))}
}

func (c geosearchKey) Bybox(width float64) geosearchBoxBybox {
	return geosearchBoxBybox{cs: append(c.cs, "BYBOX", strconv.FormatFloat(width, 'f', -1, 64))}
}

func (c geosearchKey) Asc() geosearchOrderAsc {
	return geosearchOrderAsc{cs: append(c.cs, "ASC")}
}

func (c geosearchKey) Desc() geosearchOrderDesc {
	return geosearchOrderDesc{cs: append(c.cs, "DESC")}
}

func (c geosearchKey) Count(count int64) geosearchCountCount {
	return geosearchCountCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c geosearchKey) Withcoord() geosearchWithcoordWithcoord {
	return geosearchWithcoordWithcoord{cs: append(c.cs, "WITHCOORD")}
}

func (c geosearchKey) Withdist() geosearchWithdistWithdist {
	return geosearchWithdistWithdist{cs: append(c.cs, "WITHDIST")}
}

func (c geosearchKey) Withhash() geosearchWithhashWithhash {
	return geosearchWithhashWithhash{cs: append(c.cs, "WITHHASH")}
}

type geosearchOrderAsc struct {
	cs []string
}

func (c geosearchOrderAsc) Count(count int64) geosearchCountCount {
	return geosearchCountCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c geosearchOrderAsc) Withcoord() geosearchWithcoordWithcoord {
	return geosearchWithcoordWithcoord{cs: append(c.cs, "WITHCOORD")}
}

func (c geosearchOrderAsc) Withdist() geosearchWithdistWithdist {
	return geosearchWithdistWithdist{cs: append(c.cs, "WITHDIST")}
}

func (c geosearchOrderAsc) Withhash() geosearchWithhashWithhash {
	return geosearchWithhashWithhash{cs: append(c.cs, "WITHHASH")}
}

type geosearchOrderDesc struct {
	cs []string
}

func (c geosearchOrderDesc) Count(count int64) geosearchCountCount {
	return geosearchCountCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c geosearchOrderDesc) Withcoord() geosearchWithcoordWithcoord {
	return geosearchWithcoordWithcoord{cs: append(c.cs, "WITHCOORD")}
}

func (c geosearchOrderDesc) Withdist() geosearchWithdistWithdist {
	return geosearchWithdistWithdist{cs: append(c.cs, "WITHDIST")}
}

func (c geosearchOrderDesc) Withhash() geosearchWithhashWithhash {
	return geosearchWithhashWithhash{cs: append(c.cs, "WITHHASH")}
}

type geosearchWithcoordWithcoord struct {
	cs []string
}

func (c geosearchWithcoordWithcoord) Withdist() geosearchWithdistWithdist {
	return geosearchWithdistWithdist{cs: append(c.cs, "WITHDIST")}
}

func (c geosearchWithcoordWithcoord) Withhash() geosearchWithhashWithhash {
	return geosearchWithhashWithhash{cs: append(c.cs, "WITHHASH")}
}

func (c geosearchWithcoordWithcoord) Build() []string {
	return c.cs
}

type geosearchWithdistWithdist struct {
	cs []string
}

func (c geosearchWithdistWithdist) Withhash() geosearchWithhashWithhash {
	return geosearchWithhashWithhash{cs: append(c.cs, "WITHHASH")}
}

func (c geosearchWithdistWithdist) Build() []string {
	return c.cs
}

type geosearchWithhashWithhash struct {
	cs []string
}

func (c geosearchWithhashWithhash) Build() []string {
	return c.cs
}

type geosearchstore struct {
	cs []string
}

func (c geosearchstore) Destination(destination string) geosearchstoreDestination {
	return geosearchstoreDestination{cs: append(c.cs, destination)}
}

func (b *Builder) Geosearchstore() (c geosearchstore) {
	c.cs = append(b.get(), "GEOSEARCHSTORE")
	return
}

type geosearchstoreBoxBybox struct {
	cs []string
}

func (c geosearchstoreBoxBybox) Height(height float64) geosearchstoreBoxHeight {
	return geosearchstoreBoxHeight{cs: append(c.cs, strconv.FormatFloat(height, 'f', -1, 64))}
}

type geosearchstoreBoxHeight struct {
	cs []string
}

func (c geosearchstoreBoxHeight) M() geosearchstoreBoxUnitM {
	return geosearchstoreBoxUnitM{cs: append(c.cs, "m")}
}

func (c geosearchstoreBoxHeight) Km() geosearchstoreBoxUnitKm {
	return geosearchstoreBoxUnitKm{cs: append(c.cs, "km")}
}

func (c geosearchstoreBoxHeight) Ft() geosearchstoreBoxUnitFt {
	return geosearchstoreBoxUnitFt{cs: append(c.cs, "ft")}
}

func (c geosearchstoreBoxHeight) Mi() geosearchstoreBoxUnitMi {
	return geosearchstoreBoxUnitMi{cs: append(c.cs, "mi")}
}

type geosearchstoreBoxUnitFt struct {
	cs []string
}

func (c geosearchstoreBoxUnitFt) Asc() geosearchstoreOrderAsc {
	return geosearchstoreOrderAsc{cs: append(c.cs, "ASC")}
}

func (c geosearchstoreBoxUnitFt) Desc() geosearchstoreOrderDesc {
	return geosearchstoreOrderDesc{cs: append(c.cs, "DESC")}
}

func (c geosearchstoreBoxUnitFt) Count(count int64) geosearchstoreCountCount {
	return geosearchstoreCountCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c geosearchstoreBoxUnitFt) Storedist() geosearchstoreStoredistStoredist {
	return geosearchstoreStoredistStoredist{cs: append(c.cs, "STOREDIST")}
}

type geosearchstoreBoxUnitKm struct {
	cs []string
}

func (c geosearchstoreBoxUnitKm) Asc() geosearchstoreOrderAsc {
	return geosearchstoreOrderAsc{cs: append(c.cs, "ASC")}
}

func (c geosearchstoreBoxUnitKm) Desc() geosearchstoreOrderDesc {
	return geosearchstoreOrderDesc{cs: append(c.cs, "DESC")}
}

func (c geosearchstoreBoxUnitKm) Count(count int64) geosearchstoreCountCount {
	return geosearchstoreCountCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c geosearchstoreBoxUnitKm) Storedist() geosearchstoreStoredistStoredist {
	return geosearchstoreStoredistStoredist{cs: append(c.cs, "STOREDIST")}
}

type geosearchstoreBoxUnitM struct {
	cs []string
}

func (c geosearchstoreBoxUnitM) Asc() geosearchstoreOrderAsc {
	return geosearchstoreOrderAsc{cs: append(c.cs, "ASC")}
}

func (c geosearchstoreBoxUnitM) Desc() geosearchstoreOrderDesc {
	return geosearchstoreOrderDesc{cs: append(c.cs, "DESC")}
}

func (c geosearchstoreBoxUnitM) Count(count int64) geosearchstoreCountCount {
	return geosearchstoreCountCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c geosearchstoreBoxUnitM) Storedist() geosearchstoreStoredistStoredist {
	return geosearchstoreStoredistStoredist{cs: append(c.cs, "STOREDIST")}
}

type geosearchstoreBoxUnitMi struct {
	cs []string
}

func (c geosearchstoreBoxUnitMi) Asc() geosearchstoreOrderAsc {
	return geosearchstoreOrderAsc{cs: append(c.cs, "ASC")}
}

func (c geosearchstoreBoxUnitMi) Desc() geosearchstoreOrderDesc {
	return geosearchstoreOrderDesc{cs: append(c.cs, "DESC")}
}

func (c geosearchstoreBoxUnitMi) Count(count int64) geosearchstoreCountCount {
	return geosearchstoreCountCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c geosearchstoreBoxUnitMi) Storedist() geosearchstoreStoredistStoredist {
	return geosearchstoreStoredistStoredist{cs: append(c.cs, "STOREDIST")}
}

type geosearchstoreCircleByradius struct {
	cs []string
}

func (c geosearchstoreCircleByradius) M() geosearchstoreCircleUnitM {
	return geosearchstoreCircleUnitM{cs: append(c.cs, "m")}
}

func (c geosearchstoreCircleByradius) Km() geosearchstoreCircleUnitKm {
	return geosearchstoreCircleUnitKm{cs: append(c.cs, "km")}
}

func (c geosearchstoreCircleByradius) Ft() geosearchstoreCircleUnitFt {
	return geosearchstoreCircleUnitFt{cs: append(c.cs, "ft")}
}

func (c geosearchstoreCircleByradius) Mi() geosearchstoreCircleUnitMi {
	return geosearchstoreCircleUnitMi{cs: append(c.cs, "mi")}
}

type geosearchstoreCircleUnitFt struct {
	cs []string
}

func (c geosearchstoreCircleUnitFt) Bybox(width float64) geosearchstoreBoxBybox {
	return geosearchstoreBoxBybox{cs: append(c.cs, "BYBOX", strconv.FormatFloat(width, 'f', -1, 64))}
}

func (c geosearchstoreCircleUnitFt) Asc() geosearchstoreOrderAsc {
	return geosearchstoreOrderAsc{cs: append(c.cs, "ASC")}
}

func (c geosearchstoreCircleUnitFt) Desc() geosearchstoreOrderDesc {
	return geosearchstoreOrderDesc{cs: append(c.cs, "DESC")}
}

func (c geosearchstoreCircleUnitFt) Count(count int64) geosearchstoreCountCount {
	return geosearchstoreCountCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c geosearchstoreCircleUnitFt) Storedist() geosearchstoreStoredistStoredist {
	return geosearchstoreStoredistStoredist{cs: append(c.cs, "STOREDIST")}
}

type geosearchstoreCircleUnitKm struct {
	cs []string
}

func (c geosearchstoreCircleUnitKm) Bybox(width float64) geosearchstoreBoxBybox {
	return geosearchstoreBoxBybox{cs: append(c.cs, "BYBOX", strconv.FormatFloat(width, 'f', -1, 64))}
}

func (c geosearchstoreCircleUnitKm) Asc() geosearchstoreOrderAsc {
	return geosearchstoreOrderAsc{cs: append(c.cs, "ASC")}
}

func (c geosearchstoreCircleUnitKm) Desc() geosearchstoreOrderDesc {
	return geosearchstoreOrderDesc{cs: append(c.cs, "DESC")}
}

func (c geosearchstoreCircleUnitKm) Count(count int64) geosearchstoreCountCount {
	return geosearchstoreCountCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c geosearchstoreCircleUnitKm) Storedist() geosearchstoreStoredistStoredist {
	return geosearchstoreStoredistStoredist{cs: append(c.cs, "STOREDIST")}
}

type geosearchstoreCircleUnitM struct {
	cs []string
}

func (c geosearchstoreCircleUnitM) Bybox(width float64) geosearchstoreBoxBybox {
	return geosearchstoreBoxBybox{cs: append(c.cs, "BYBOX", strconv.FormatFloat(width, 'f', -1, 64))}
}

func (c geosearchstoreCircleUnitM) Asc() geosearchstoreOrderAsc {
	return geosearchstoreOrderAsc{cs: append(c.cs, "ASC")}
}

func (c geosearchstoreCircleUnitM) Desc() geosearchstoreOrderDesc {
	return geosearchstoreOrderDesc{cs: append(c.cs, "DESC")}
}

func (c geosearchstoreCircleUnitM) Count(count int64) geosearchstoreCountCount {
	return geosearchstoreCountCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c geosearchstoreCircleUnitM) Storedist() geosearchstoreStoredistStoredist {
	return geosearchstoreStoredistStoredist{cs: append(c.cs, "STOREDIST")}
}

type geosearchstoreCircleUnitMi struct {
	cs []string
}

func (c geosearchstoreCircleUnitMi) Bybox(width float64) geosearchstoreBoxBybox {
	return geosearchstoreBoxBybox{cs: append(c.cs, "BYBOX", strconv.FormatFloat(width, 'f', -1, 64))}
}

func (c geosearchstoreCircleUnitMi) Asc() geosearchstoreOrderAsc {
	return geosearchstoreOrderAsc{cs: append(c.cs, "ASC")}
}

func (c geosearchstoreCircleUnitMi) Desc() geosearchstoreOrderDesc {
	return geosearchstoreOrderDesc{cs: append(c.cs, "DESC")}
}

func (c geosearchstoreCircleUnitMi) Count(count int64) geosearchstoreCountCount {
	return geosearchstoreCountCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c geosearchstoreCircleUnitMi) Storedist() geosearchstoreStoredistStoredist {
	return geosearchstoreStoredistStoredist{cs: append(c.cs, "STOREDIST")}
}

type geosearchstoreCountAnyAny struct {
	cs []string
}

func (c geosearchstoreCountAnyAny) Storedist() geosearchstoreStoredistStoredist {
	return geosearchstoreStoredistStoredist{cs: append(c.cs, "STOREDIST")}
}

func (c geosearchstoreCountAnyAny) Build() []string {
	return c.cs
}

type geosearchstoreCountCount struct {
	cs []string
}

func (c geosearchstoreCountCount) Any() geosearchstoreCountAnyAny {
	return geosearchstoreCountAnyAny{cs: append(c.cs, "ANY")}
}

func (c geosearchstoreCountCount) Storedist() geosearchstoreStoredistStoredist {
	return geosearchstoreStoredistStoredist{cs: append(c.cs, "STOREDIST")}
}

func (c geosearchstoreCountCount) Build() []string {
	return c.cs
}

type geosearchstoreDestination struct {
	cs []string
}

func (c geosearchstoreDestination) Source(source string) geosearchstoreSource {
	return geosearchstoreSource{cs: append(c.cs, source)}
}

type geosearchstoreFromlonlat struct {
	cs []string
}

func (c geosearchstoreFromlonlat) Byradius(radius float64) geosearchstoreCircleByradius {
	return geosearchstoreCircleByradius{cs: append(c.cs, "BYRADIUS", strconv.FormatFloat(radius, 'f', -1, 64))}
}

func (c geosearchstoreFromlonlat) Bybox(width float64) geosearchstoreBoxBybox {
	return geosearchstoreBoxBybox{cs: append(c.cs, "BYBOX", strconv.FormatFloat(width, 'f', -1, 64))}
}

func (c geosearchstoreFromlonlat) Asc() geosearchstoreOrderAsc {
	return geosearchstoreOrderAsc{cs: append(c.cs, "ASC")}
}

func (c geosearchstoreFromlonlat) Desc() geosearchstoreOrderDesc {
	return geosearchstoreOrderDesc{cs: append(c.cs, "DESC")}
}

func (c geosearchstoreFromlonlat) Count(count int64) geosearchstoreCountCount {
	return geosearchstoreCountCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c geosearchstoreFromlonlat) Storedist() geosearchstoreStoredistStoredist {
	return geosearchstoreStoredistStoredist{cs: append(c.cs, "STOREDIST")}
}

type geosearchstoreFrommember struct {
	cs []string
}

func (c geosearchstoreFrommember) Fromlonlat(longitude float64, latitude float64) geosearchstoreFromlonlat {
	return geosearchstoreFromlonlat{cs: append(c.cs, "FROMLONLAT", strconv.FormatFloat(longitude, 'f', -1, 64), strconv.FormatFloat(latitude, 'f', -1, 64))}
}

func (c geosearchstoreFrommember) Byradius(radius float64) geosearchstoreCircleByradius {
	return geosearchstoreCircleByradius{cs: append(c.cs, "BYRADIUS", strconv.FormatFloat(radius, 'f', -1, 64))}
}

func (c geosearchstoreFrommember) Bybox(width float64) geosearchstoreBoxBybox {
	return geosearchstoreBoxBybox{cs: append(c.cs, "BYBOX", strconv.FormatFloat(width, 'f', -1, 64))}
}

func (c geosearchstoreFrommember) Asc() geosearchstoreOrderAsc {
	return geosearchstoreOrderAsc{cs: append(c.cs, "ASC")}
}

func (c geosearchstoreFrommember) Desc() geosearchstoreOrderDesc {
	return geosearchstoreOrderDesc{cs: append(c.cs, "DESC")}
}

func (c geosearchstoreFrommember) Count(count int64) geosearchstoreCountCount {
	return geosearchstoreCountCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c geosearchstoreFrommember) Storedist() geosearchstoreStoredistStoredist {
	return geosearchstoreStoredistStoredist{cs: append(c.cs, "STOREDIST")}
}

type geosearchstoreOrderAsc struct {
	cs []string
}

func (c geosearchstoreOrderAsc) Count(count int64) geosearchstoreCountCount {
	return geosearchstoreCountCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c geosearchstoreOrderAsc) Storedist() geosearchstoreStoredistStoredist {
	return geosearchstoreStoredistStoredist{cs: append(c.cs, "STOREDIST")}
}

type geosearchstoreOrderDesc struct {
	cs []string
}

func (c geosearchstoreOrderDesc) Count(count int64) geosearchstoreCountCount {
	return geosearchstoreCountCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c geosearchstoreOrderDesc) Storedist() geosearchstoreStoredistStoredist {
	return geosearchstoreStoredistStoredist{cs: append(c.cs, "STOREDIST")}
}

type geosearchstoreSource struct {
	cs []string
}

func (c geosearchstoreSource) Frommember(member string) geosearchstoreFrommember {
	return geosearchstoreFrommember{cs: append(c.cs, "FROMMEMBER", member)}
}

func (c geosearchstoreSource) Fromlonlat(longitude float64, latitude float64) geosearchstoreFromlonlat {
	return geosearchstoreFromlonlat{cs: append(c.cs, "FROMLONLAT", strconv.FormatFloat(longitude, 'f', -1, 64), strconv.FormatFloat(latitude, 'f', -1, 64))}
}

func (c geosearchstoreSource) Byradius(radius float64) geosearchstoreCircleByradius {
	return geosearchstoreCircleByradius{cs: append(c.cs, "BYRADIUS", strconv.FormatFloat(radius, 'f', -1, 64))}
}

func (c geosearchstoreSource) Bybox(width float64) geosearchstoreBoxBybox {
	return geosearchstoreBoxBybox{cs: append(c.cs, "BYBOX", strconv.FormatFloat(width, 'f', -1, 64))}
}

func (c geosearchstoreSource) Asc() geosearchstoreOrderAsc {
	return geosearchstoreOrderAsc{cs: append(c.cs, "ASC")}
}

func (c geosearchstoreSource) Desc() geosearchstoreOrderDesc {
	return geosearchstoreOrderDesc{cs: append(c.cs, "DESC")}
}

func (c geosearchstoreSource) Count(count int64) geosearchstoreCountCount {
	return geosearchstoreCountCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c geosearchstoreSource) Storedist() geosearchstoreStoredistStoredist {
	return geosearchstoreStoredistStoredist{cs: append(c.cs, "STOREDIST")}
}

type geosearchstoreStoredistStoredist struct {
	cs []string
}

func (c geosearchstoreStoredistStoredist) Build() []string {
	return c.cs
}

type get struct {
	cs []string
}

func (c get) Key(key string) getKey {
	return getKey{cs: append(c.cs, key)}
}

func (b *Builder) Get() (c get) {
	c.cs = append(b.get(), "GET")
	return
}

type getKey struct {
	cs []string
}

func (c getKey) Build() []string {
	return c.cs
}

type getbit struct {
	cs []string
}

func (c getbit) Key(key string) getbitKey {
	return getbitKey{cs: append(c.cs, key)}
}

func (b *Builder) Getbit() (c getbit) {
	c.cs = append(b.get(), "GETBIT")
	return
}

type getbitKey struct {
	cs []string
}

func (c getbitKey) Offset(offset int64) getbitOffset {
	return getbitOffset{cs: append(c.cs, strconv.FormatInt(offset, 10))}
}

type getbitOffset struct {
	cs []string
}

func (c getbitOffset) Build() []string {
	return c.cs
}

type getdel struct {
	cs []string
}

func (c getdel) Key(key string) getdelKey {
	return getdelKey{cs: append(c.cs, key)}
}

func (b *Builder) Getdel() (c getdel) {
	c.cs = append(b.get(), "GETDEL")
	return
}

type getdelKey struct {
	cs []string
}

func (c getdelKey) Build() []string {
	return c.cs
}

type getex struct {
	cs []string
}

func (c getex) Key(key string) getexKey {
	return getexKey{cs: append(c.cs, key)}
}

func (b *Builder) Getex() (c getex) {
	c.cs = append(b.get(), "GETEX")
	return
}

type getexExpirationEx struct {
	cs []string
}

func (c getexExpirationEx) Build() []string {
	return c.cs
}

type getexExpirationExat struct {
	cs []string
}

func (c getexExpirationExat) Build() []string {
	return c.cs
}

type getexExpirationPersist struct {
	cs []string
}

func (c getexExpirationPersist) Build() []string {
	return c.cs
}

type getexExpirationPx struct {
	cs []string
}

func (c getexExpirationPx) Build() []string {
	return c.cs
}

type getexExpirationPxat struct {
	cs []string
}

func (c getexExpirationPxat) Build() []string {
	return c.cs
}

type getexKey struct {
	cs []string
}

func (c getexKey) Ex(seconds int64) getexExpirationEx {
	return getexExpirationEx{cs: append(c.cs, "EX", strconv.FormatInt(seconds, 10))}
}

func (c getexKey) Px(milliseconds int64) getexExpirationPx {
	return getexExpirationPx{cs: append(c.cs, "PX", strconv.FormatInt(milliseconds, 10))}
}

func (c getexKey) Exat(timestamp int64) getexExpirationExat {
	return getexExpirationExat{cs: append(c.cs, "EXAT", strconv.FormatInt(timestamp, 10))}
}

func (c getexKey) Pxat(millisecondstimestamp int64) getexExpirationPxat {
	return getexExpirationPxat{cs: append(c.cs, "PXAT", strconv.FormatInt(millisecondstimestamp, 10))}
}

func (c getexKey) Persist() getexExpirationPersist {
	return getexExpirationPersist{cs: append(c.cs, "PERSIST")}
}

func (c getexKey) Build() []string {
	return c.cs
}

type getrange struct {
	cs []string
}

func (c getrange) Key(key string) getrangeKey {
	return getrangeKey{cs: append(c.cs, key)}
}

func (b *Builder) Getrange() (c getrange) {
	c.cs = append(b.get(), "GETRANGE")
	return
}

type getrangeEnd struct {
	cs []string
}

func (c getrangeEnd) Build() []string {
	return c.cs
}

type getrangeKey struct {
	cs []string
}

func (c getrangeKey) Start(start int64) getrangeStart {
	return getrangeStart{cs: append(c.cs, strconv.FormatInt(start, 10))}
}

type getrangeStart struct {
	cs []string
}

func (c getrangeStart) End(end int64) getrangeEnd {
	return getrangeEnd{cs: append(c.cs, strconv.FormatInt(end, 10))}
}

type getset struct {
	cs []string
}

func (c getset) Key(key string) getsetKey {
	return getsetKey{cs: append(c.cs, key)}
}

func (b *Builder) Getset() (c getset) {
	c.cs = append(b.get(), "GETSET")
	return
}

type getsetKey struct {
	cs []string
}

func (c getsetKey) Value(value string) getsetValue {
	return getsetValue{cs: append(c.cs, value)}
}

type getsetValue struct {
	cs []string
}

func (c getsetValue) Build() []string {
	return c.cs
}

type hdel struct {
	cs []string
}

func (c hdel) Key(key string) hdelKey {
	return hdelKey{cs: append(c.cs, key)}
}

func (b *Builder) Hdel() (c hdel) {
	c.cs = append(b.get(), "HDEL")
	return
}

type hdelField struct {
	cs []string
}

func (c hdelField) Field(field ...string) hdelField {
	return hdelField{cs: append(c.cs, field...)}
}

func (c hdelField) Build() []string {
	return c.cs
}

type hdelKey struct {
	cs []string
}

func (c hdelKey) Field(field ...string) hdelField {
	return hdelField{cs: append(c.cs, field...)}
}

type hello struct {
	cs []string
}

func (c hello) Protover(protover int64) helloArgumentsProtover {
	return helloArgumentsProtover{cs: append(c.cs, strconv.FormatInt(protover, 10))}
}

func (b *Builder) Hello() (c hello) {
	c.cs = append(b.get(), "HELLO")
	return
}

type helloArgumentsAuth struct {
	cs []string
}

func (c helloArgumentsAuth) Setname(clientname string) helloArgumentsSetname {
	return helloArgumentsSetname{cs: append(c.cs, "SETNAME", clientname)}
}

func (c helloArgumentsAuth) Build() []string {
	return c.cs
}

type helloArgumentsProtover struct {
	cs []string
}

func (c helloArgumentsProtover) Auth(username string, password string) helloArgumentsAuth {
	return helloArgumentsAuth{cs: append(c.cs, "AUTH", username, password)}
}

func (c helloArgumentsProtover) Setname(clientname string) helloArgumentsSetname {
	return helloArgumentsSetname{cs: append(c.cs, "SETNAME", clientname)}
}

func (c helloArgumentsProtover) Build() []string {
	return c.cs
}

type helloArgumentsSetname struct {
	cs []string
}

func (c helloArgumentsSetname) Build() []string {
	return c.cs
}

type hexists struct {
	cs []string
}

func (c hexists) Key(key string) hexistsKey {
	return hexistsKey{cs: append(c.cs, key)}
}

func (b *Builder) Hexists() (c hexists) {
	c.cs = append(b.get(), "HEXISTS")
	return
}

type hexistsField struct {
	cs []string
}

func (c hexistsField) Build() []string {
	return c.cs
}

type hexistsKey struct {
	cs []string
}

func (c hexistsKey) Field(field string) hexistsField {
	return hexistsField{cs: append(c.cs, field)}
}

type hget struct {
	cs []string
}

func (c hget) Key(key string) hgetKey {
	return hgetKey{cs: append(c.cs, key)}
}

func (b *Builder) Hget() (c hget) {
	c.cs = append(b.get(), "HGET")
	return
}

type hgetField struct {
	cs []string
}

func (c hgetField) Build() []string {
	return c.cs
}

type hgetKey struct {
	cs []string
}

func (c hgetKey) Field(field string) hgetField {
	return hgetField{cs: append(c.cs, field)}
}

type hgetall struct {
	cs []string
}

func (c hgetall) Key(key string) hgetallKey {
	return hgetallKey{cs: append(c.cs, key)}
}

func (b *Builder) Hgetall() (c hgetall) {
	c.cs = append(b.get(), "HGETALL")
	return
}

type hgetallKey struct {
	cs []string
}

func (c hgetallKey) Build() []string {
	return c.cs
}

type hincrby struct {
	cs []string
}

func (c hincrby) Key(key string) hincrbyKey {
	return hincrbyKey{cs: append(c.cs, key)}
}

func (b *Builder) Hincrby() (c hincrby) {
	c.cs = append(b.get(), "HINCRBY")
	return
}

type hincrbyField struct {
	cs []string
}

func (c hincrbyField) Increment(increment int64) hincrbyIncrement {
	return hincrbyIncrement{cs: append(c.cs, strconv.FormatInt(increment, 10))}
}

type hincrbyIncrement struct {
	cs []string
}

func (c hincrbyIncrement) Build() []string {
	return c.cs
}

type hincrbyKey struct {
	cs []string
}

func (c hincrbyKey) Field(field string) hincrbyField {
	return hincrbyField{cs: append(c.cs, field)}
}

type hincrbyfloat struct {
	cs []string
}

func (c hincrbyfloat) Key(key string) hincrbyfloatKey {
	return hincrbyfloatKey{cs: append(c.cs, key)}
}

func (b *Builder) Hincrbyfloat() (c hincrbyfloat) {
	c.cs = append(b.get(), "HINCRBYFLOAT")
	return
}

type hincrbyfloatField struct {
	cs []string
}

func (c hincrbyfloatField) Increment(increment float64) hincrbyfloatIncrement {
	return hincrbyfloatIncrement{cs: append(c.cs, strconv.FormatFloat(increment, 'f', -1, 64))}
}

type hincrbyfloatIncrement struct {
	cs []string
}

func (c hincrbyfloatIncrement) Build() []string {
	return c.cs
}

type hincrbyfloatKey struct {
	cs []string
}

func (c hincrbyfloatKey) Field(field string) hincrbyfloatField {
	return hincrbyfloatField{cs: append(c.cs, field)}
}

type hkeys struct {
	cs []string
}

func (c hkeys) Key(key string) hkeysKey {
	return hkeysKey{cs: append(c.cs, key)}
}

func (b *Builder) Hkeys() (c hkeys) {
	c.cs = append(b.get(), "HKEYS")
	return
}

type hkeysKey struct {
	cs []string
}

func (c hkeysKey) Build() []string {
	return c.cs
}

type hlen struct {
	cs []string
}

func (c hlen) Key(key string) hlenKey {
	return hlenKey{cs: append(c.cs, key)}
}

func (b *Builder) Hlen() (c hlen) {
	c.cs = append(b.get(), "HLEN")
	return
}

type hlenKey struct {
	cs []string
}

func (c hlenKey) Build() []string {
	return c.cs
}

type hmget struct {
	cs []string
}

func (c hmget) Key(key string) hmgetKey {
	return hmgetKey{cs: append(c.cs, key)}
}

func (b *Builder) Hmget() (c hmget) {
	c.cs = append(b.get(), "HMGET")
	return
}

type hmgetField struct {
	cs []string
}

func (c hmgetField) Field(field ...string) hmgetField {
	return hmgetField{cs: append(c.cs, field...)}
}

func (c hmgetField) Build() []string {
	return c.cs
}

type hmgetKey struct {
	cs []string
}

func (c hmgetKey) Field(field ...string) hmgetField {
	return hmgetField{cs: append(c.cs, field...)}
}

type hmset struct {
	cs []string
}

func (c hmset) Key(key string) hmsetKey {
	return hmsetKey{cs: append(c.cs, key)}
}

func (b *Builder) Hmset() (c hmset) {
	c.cs = append(b.get(), "HMSET")
	return
}

type hmsetFieldValue struct {
	cs []string
}

func (c hmsetFieldValue) FieldValue(field string, value string) hmsetFieldValue {
	return hmsetFieldValue{cs: append(c.cs, field, value)}
}

func (c hmsetFieldValue) Build() []string {
	return c.cs
}

type hmsetKey struct {
	cs []string
}

func (c hmsetKey) FieldValue() hmsetFieldValue {
	return hmsetFieldValue{cs: append(c.cs)}
}

type hrandfield struct {
	cs []string
}

func (c hrandfield) Key(key string) hrandfieldKey {
	return hrandfieldKey{cs: append(c.cs, key)}
}

func (b *Builder) Hrandfield() (c hrandfield) {
	c.cs = append(b.get(), "HRANDFIELD")
	return
}

type hrandfieldKey struct {
	cs []string
}

func (c hrandfieldKey) Count(count int64) hrandfieldOptionsCount {
	return hrandfieldOptionsCount{cs: append(c.cs, strconv.FormatInt(count, 10))}
}

type hrandfieldOptionsCount struct {
	cs []string
}

func (c hrandfieldOptionsCount) Withvalues() hrandfieldOptionsWithvaluesWithvalues {
	return hrandfieldOptionsWithvaluesWithvalues{cs: append(c.cs, "WITHVALUES")}
}

func (c hrandfieldOptionsCount) Build() []string {
	return c.cs
}

type hrandfieldOptionsWithvaluesWithvalues struct {
	cs []string
}

func (c hrandfieldOptionsWithvaluesWithvalues) Build() []string {
	return c.cs
}

type hscan struct {
	cs []string
}

func (c hscan) Key(key string) hscanKey {
	return hscanKey{cs: append(c.cs, key)}
}

func (b *Builder) Hscan() (c hscan) {
	c.cs = append(b.get(), "HSCAN")
	return
}

type hscanCount struct {
	cs []string
}

func (c hscanCount) Build() []string {
	return c.cs
}

type hscanCursor struct {
	cs []string
}

func (c hscanCursor) Match(pattern string) hscanMatch {
	return hscanMatch{cs: append(c.cs, "MATCH", pattern)}
}

func (c hscanCursor) Count(count int64) hscanCount {
	return hscanCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c hscanCursor) Build() []string {
	return c.cs
}

type hscanKey struct {
	cs []string
}

func (c hscanKey) Cursor(cursor int64) hscanCursor {
	return hscanCursor{cs: append(c.cs, strconv.FormatInt(cursor, 10))}
}

type hscanMatch struct {
	cs []string
}

func (c hscanMatch) Count(count int64) hscanCount {
	return hscanCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c hscanMatch) Build() []string {
	return c.cs
}

type hset struct {
	cs []string
}

func (c hset) Key(key string) hsetKey {
	return hsetKey{cs: append(c.cs, key)}
}

func (b *Builder) Hset() (c hset) {
	c.cs = append(b.get(), "HSET")
	return
}

type hsetFieldValue struct {
	cs []string
}

func (c hsetFieldValue) FieldValue(field string, value string) hsetFieldValue {
	return hsetFieldValue{cs: append(c.cs, field, value)}
}

func (c hsetFieldValue) Build() []string {
	return c.cs
}

type hsetKey struct {
	cs []string
}

func (c hsetKey) FieldValue() hsetFieldValue {
	return hsetFieldValue{cs: append(c.cs)}
}

type hsetnx struct {
	cs []string
}

func (c hsetnx) Key(key string) hsetnxKey {
	return hsetnxKey{cs: append(c.cs, key)}
}

func (b *Builder) Hsetnx() (c hsetnx) {
	c.cs = append(b.get(), "HSETNX")
	return
}

type hsetnxField struct {
	cs []string
}

func (c hsetnxField) Value(value string) hsetnxValue {
	return hsetnxValue{cs: append(c.cs, value)}
}

type hsetnxKey struct {
	cs []string
}

func (c hsetnxKey) Field(field string) hsetnxField {
	return hsetnxField{cs: append(c.cs, field)}
}

type hsetnxValue struct {
	cs []string
}

func (c hsetnxValue) Build() []string {
	return c.cs
}

type hstrlen struct {
	cs []string
}

func (c hstrlen) Key(key string) hstrlenKey {
	return hstrlenKey{cs: append(c.cs, key)}
}

func (b *Builder) Hstrlen() (c hstrlen) {
	c.cs = append(b.get(), "HSTRLEN")
	return
}

type hstrlenField struct {
	cs []string
}

func (c hstrlenField) Build() []string {
	return c.cs
}

type hstrlenKey struct {
	cs []string
}

func (c hstrlenKey) Field(field string) hstrlenField {
	return hstrlenField{cs: append(c.cs, field)}
}

type hvals struct {
	cs []string
}

func (c hvals) Key(key string) hvalsKey {
	return hvalsKey{cs: append(c.cs, key)}
}

func (b *Builder) Hvals() (c hvals) {
	c.cs = append(b.get(), "HVALS")
	return
}

type hvalsKey struct {
	cs []string
}

func (c hvalsKey) Build() []string {
	return c.cs
}

type incr struct {
	cs []string
}

func (c incr) Key(key string) incrKey {
	return incrKey{cs: append(c.cs, key)}
}

func (b *Builder) Incr() (c incr) {
	c.cs = append(b.get(), "INCR")
	return
}

type incrKey struct {
	cs []string
}

func (c incrKey) Build() []string {
	return c.cs
}

type incrby struct {
	cs []string
}

func (c incrby) Key(key string) incrbyKey {
	return incrbyKey{cs: append(c.cs, key)}
}

func (b *Builder) Incrby() (c incrby) {
	c.cs = append(b.get(), "INCRBY")
	return
}

type incrbyIncrement struct {
	cs []string
}

func (c incrbyIncrement) Build() []string {
	return c.cs
}

type incrbyKey struct {
	cs []string
}

func (c incrbyKey) Increment(increment int64) incrbyIncrement {
	return incrbyIncrement{cs: append(c.cs, strconv.FormatInt(increment, 10))}
}

type incrbyfloat struct {
	cs []string
}

func (c incrbyfloat) Key(key string) incrbyfloatKey {
	return incrbyfloatKey{cs: append(c.cs, key)}
}

func (b *Builder) Incrbyfloat() (c incrbyfloat) {
	c.cs = append(b.get(), "INCRBYFLOAT")
	return
}

type incrbyfloatIncrement struct {
	cs []string
}

func (c incrbyfloatIncrement) Build() []string {
	return c.cs
}

type incrbyfloatKey struct {
	cs []string
}

func (c incrbyfloatKey) Increment(increment float64) incrbyfloatIncrement {
	return incrbyfloatIncrement{cs: append(c.cs, strconv.FormatFloat(increment, 'f', -1, 64))}
}

type info struct {
	cs []string
}

func (c info) Section(section string) infoSection {
	return infoSection{cs: append(c.cs, section)}
}

func (c info) Build() []string {
	return c.cs
}

func (b *Builder) Info() (c info) {
	c.cs = append(b.get(), "INFO")
	return
}

type infoSection struct {
	cs []string
}

func (c infoSection) Build() []string {
	return c.cs
}

type keys struct {
	cs []string
}

func (c keys) Pattern(pattern string) keysPattern {
	return keysPattern{cs: append(c.cs, pattern)}
}

func (b *Builder) Keys() (c keys) {
	c.cs = append(b.get(), "KEYS")
	return
}

type keysPattern struct {
	cs []string
}

func (c keysPattern) Build() []string {
	return c.cs
}

type lastsave struct {
	cs []string
}

func (c lastsave) Build() []string {
	return c.cs
}

func (b *Builder) Lastsave() (c lastsave) {
	c.cs = append(b.get(), "LASTSAVE")
	return
}

type latencyDoctor struct {
	cs []string
}

func (c latencyDoctor) Build() []string {
	return c.cs
}

func (b *Builder) LatencyDoctor() (c latencyDoctor) {
	c.cs = append(b.get(), "LATENCY", "DOCTOR")
	return
}

type latencyGraph struct {
	cs []string
}

func (c latencyGraph) Event(event string) latencyGraphEvent {
	return latencyGraphEvent{cs: append(c.cs, event)}
}

func (b *Builder) LatencyGraph() (c latencyGraph) {
	c.cs = append(b.get(), "LATENCY", "GRAPH")
	return
}

type latencyGraphEvent struct {
	cs []string
}

func (c latencyGraphEvent) Build() []string {
	return c.cs
}

type latencyHelp struct {
	cs []string
}

func (c latencyHelp) Build() []string {
	return c.cs
}

func (b *Builder) LatencyHelp() (c latencyHelp) {
	c.cs = append(b.get(), "LATENCY", "HELP")
	return
}

type latencyHistory struct {
	cs []string
}

func (c latencyHistory) Event(event string) latencyHistoryEvent {
	return latencyHistoryEvent{cs: append(c.cs, event)}
}

func (b *Builder) LatencyHistory() (c latencyHistory) {
	c.cs = append(b.get(), "LATENCY", "HISTORY")
	return
}

type latencyHistoryEvent struct {
	cs []string
}

func (c latencyHistoryEvent) Build() []string {
	return c.cs
}

type latencyLatest struct {
	cs []string
}

func (c latencyLatest) Build() []string {
	return c.cs
}

func (b *Builder) LatencyLatest() (c latencyLatest) {
	c.cs = append(b.get(), "LATENCY", "LATEST")
	return
}

type latencyReset struct {
	cs []string
}

func (c latencyReset) Event(event ...string) latencyResetEvent {
	return latencyResetEvent{cs: append(c.cs, event...)}
}

func (c latencyReset) Build() []string {
	return c.cs
}

func (b *Builder) LatencyReset() (c latencyReset) {
	c.cs = append(b.get(), "LATENCY", "RESET")
	return
}

type latencyResetEvent struct {
	cs []string
}

func (c latencyResetEvent) Event(event ...string) latencyResetEvent {
	return latencyResetEvent{cs: append(c.cs, event...)}
}

func (c latencyResetEvent) Build() []string {
	return c.cs
}

type lindex struct {
	cs []string
}

func (c lindex) Key(key string) lindexKey {
	return lindexKey{cs: append(c.cs, key)}
}

func (b *Builder) Lindex() (c lindex) {
	c.cs = append(b.get(), "LINDEX")
	return
}

type lindexIndex struct {
	cs []string
}

func (c lindexIndex) Build() []string {
	return c.cs
}

type lindexKey struct {
	cs []string
}

func (c lindexKey) Index(index int64) lindexIndex {
	return lindexIndex{cs: append(c.cs, strconv.FormatInt(index, 10))}
}

type linsert struct {
	cs []string
}

func (c linsert) Key(key string) linsertKey {
	return linsertKey{cs: append(c.cs, key)}
}

func (b *Builder) Linsert() (c linsert) {
	c.cs = append(b.get(), "LINSERT")
	return
}

type linsertElement struct {
	cs []string
}

func (c linsertElement) Build() []string {
	return c.cs
}

type linsertKey struct {
	cs []string
}

func (c linsertKey) Before() linsertWhereBefore {
	return linsertWhereBefore{cs: append(c.cs, "BEFORE")}
}

func (c linsertKey) After() linsertWhereAfter {
	return linsertWhereAfter{cs: append(c.cs, "AFTER")}
}

type linsertPivot struct {
	cs []string
}

func (c linsertPivot) Element(element string) linsertElement {
	return linsertElement{cs: append(c.cs, element)}
}

type linsertWhereAfter struct {
	cs []string
}

func (c linsertWhereAfter) Pivot(pivot string) linsertPivot {
	return linsertPivot{cs: append(c.cs, pivot)}
}

type linsertWhereBefore struct {
	cs []string
}

func (c linsertWhereBefore) Pivot(pivot string) linsertPivot {
	return linsertPivot{cs: append(c.cs, pivot)}
}

type llen struct {
	cs []string
}

func (c llen) Key(key string) llenKey {
	return llenKey{cs: append(c.cs, key)}
}

func (b *Builder) Llen() (c llen) {
	c.cs = append(b.get(), "LLEN")
	return
}

type llenKey struct {
	cs []string
}

func (c llenKey) Build() []string {
	return c.cs
}

type lmove struct {
	cs []string
}

func (c lmove) Source(source string) lmoveSource {
	return lmoveSource{cs: append(c.cs, source)}
}

func (b *Builder) Lmove() (c lmove) {
	c.cs = append(b.get(), "LMOVE")
	return
}

type lmoveDestination struct {
	cs []string
}

func (c lmoveDestination) Left() lmoveWherefromLeft {
	return lmoveWherefromLeft{cs: append(c.cs, "LEFT")}
}

func (c lmoveDestination) Right() lmoveWherefromRight {
	return lmoveWherefromRight{cs: append(c.cs, "RIGHT")}
}

type lmoveSource struct {
	cs []string
}

func (c lmoveSource) Destination(destination string) lmoveDestination {
	return lmoveDestination{cs: append(c.cs, destination)}
}

type lmoveWherefromLeft struct {
	cs []string
}

func (c lmoveWherefromLeft) Left() lmoveWheretoLeft {
	return lmoveWheretoLeft{cs: append(c.cs, "LEFT")}
}

func (c lmoveWherefromLeft) Right() lmoveWheretoRight {
	return lmoveWheretoRight{cs: append(c.cs, "RIGHT")}
}

type lmoveWherefromRight struct {
	cs []string
}

func (c lmoveWherefromRight) Left() lmoveWheretoLeft {
	return lmoveWheretoLeft{cs: append(c.cs, "LEFT")}
}

func (c lmoveWherefromRight) Right() lmoveWheretoRight {
	return lmoveWheretoRight{cs: append(c.cs, "RIGHT")}
}

type lmoveWheretoLeft struct {
	cs []string
}

func (c lmoveWheretoLeft) Build() []string {
	return c.cs
}

type lmoveWheretoRight struct {
	cs []string
}

func (c lmoveWheretoRight) Build() []string {
	return c.cs
}

type lmpop struct {
	cs []string
}

func (c lmpop) Numkeys(numkeys int64) lmpopNumkeys {
	return lmpopNumkeys{cs: append(c.cs, strconv.FormatInt(numkeys, 10))}
}

func (b *Builder) Lmpop() (c lmpop) {
	c.cs = append(b.get(), "LMPOP")
	return
}

type lmpopCount struct {
	cs []string
}

func (c lmpopCount) Build() []string {
	return c.cs
}

type lmpopKey struct {
	cs []string
}

func (c lmpopKey) Left() lmpopWhereLeft {
	return lmpopWhereLeft{cs: append(c.cs, "LEFT")}
}

func (c lmpopKey) Right() lmpopWhereRight {
	return lmpopWhereRight{cs: append(c.cs, "RIGHT")}
}

func (c lmpopKey) Key(key ...string) lmpopKey {
	return lmpopKey{cs: append(c.cs, key...)}
}

type lmpopNumkeys struct {
	cs []string
}

func (c lmpopNumkeys) Key(key ...string) lmpopKey {
	return lmpopKey{cs: append(c.cs, key...)}
}

func (c lmpopNumkeys) Left() lmpopWhereLeft {
	return lmpopWhereLeft{cs: append(c.cs, "LEFT")}
}

func (c lmpopNumkeys) Right() lmpopWhereRight {
	return lmpopWhereRight{cs: append(c.cs, "RIGHT")}
}

type lmpopWhereLeft struct {
	cs []string
}

func (c lmpopWhereLeft) Count(count int64) lmpopCount {
	return lmpopCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c lmpopWhereLeft) Build() []string {
	return c.cs
}

type lmpopWhereRight struct {
	cs []string
}

func (c lmpopWhereRight) Count(count int64) lmpopCount {
	return lmpopCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c lmpopWhereRight) Build() []string {
	return c.cs
}

type lolwut struct {
	cs []string
}

func (c lolwut) Version(version int64) lolwutVersion {
	return lolwutVersion{cs: append(c.cs, "VERSION", strconv.FormatInt(version, 10))}
}

func (c lolwut) Build() []string {
	return c.cs
}

func (b *Builder) Lolwut() (c lolwut) {
	c.cs = append(b.get(), "LOLWUT")
	return
}

type lolwutVersion struct {
	cs []string
}

func (c lolwutVersion) Build() []string {
	return c.cs
}

type lpop struct {
	cs []string
}

func (c lpop) Key(key string) lpopKey {
	return lpopKey{cs: append(c.cs, key)}
}

func (b *Builder) Lpop() (c lpop) {
	c.cs = append(b.get(), "LPOP")
	return
}

type lpopCount struct {
	cs []string
}

func (c lpopCount) Build() []string {
	return c.cs
}

type lpopKey struct {
	cs []string
}

func (c lpopKey) Count(count int64) lpopCount {
	return lpopCount{cs: append(c.cs, strconv.FormatInt(count, 10))}
}

func (c lpopKey) Build() []string {
	return c.cs
}

type lpos struct {
	cs []string
}

func (c lpos) Key(key string) lposKey {
	return lposKey{cs: append(c.cs, key)}
}

func (b *Builder) Lpos() (c lpos) {
	c.cs = append(b.get(), "LPOS")
	return
}

type lposCount struct {
	cs []string
}

func (c lposCount) Maxlen(len int64) lposMaxlen {
	return lposMaxlen{cs: append(c.cs, "MAXLEN", strconv.FormatInt(len, 10))}
}

func (c lposCount) Build() []string {
	return c.cs
}

type lposElement struct {
	cs []string
}

func (c lposElement) Rank(rank int64) lposRank {
	return lposRank{cs: append(c.cs, "RANK", strconv.FormatInt(rank, 10))}
}

func (c lposElement) Count(numMatches int64) lposCount {
	return lposCount{cs: append(c.cs, "COUNT", strconv.FormatInt(numMatches, 10))}
}

func (c lposElement) Maxlen(len int64) lposMaxlen {
	return lposMaxlen{cs: append(c.cs, "MAXLEN", strconv.FormatInt(len, 10))}
}

func (c lposElement) Build() []string {
	return c.cs
}

type lposKey struct {
	cs []string
}

func (c lposKey) Element(element string) lposElement {
	return lposElement{cs: append(c.cs, element)}
}

type lposMaxlen struct {
	cs []string
}

func (c lposMaxlen) Build() []string {
	return c.cs
}

type lposRank struct {
	cs []string
}

func (c lposRank) Count(numMatches int64) lposCount {
	return lposCount{cs: append(c.cs, "COUNT", strconv.FormatInt(numMatches, 10))}
}

func (c lposRank) Maxlen(len int64) lposMaxlen {
	return lposMaxlen{cs: append(c.cs, "MAXLEN", strconv.FormatInt(len, 10))}
}

func (c lposRank) Build() []string {
	return c.cs
}

type lpush struct {
	cs []string
}

func (c lpush) Key(key string) lpushKey {
	return lpushKey{cs: append(c.cs, key)}
}

func (b *Builder) Lpush() (c lpush) {
	c.cs = append(b.get(), "LPUSH")
	return
}

type lpushElement struct {
	cs []string
}

func (c lpushElement) Element(element ...string) lpushElement {
	return lpushElement{cs: append(c.cs, element...)}
}

func (c lpushElement) Build() []string {
	return c.cs
}

type lpushKey struct {
	cs []string
}

func (c lpushKey) Element(element ...string) lpushElement {
	return lpushElement{cs: append(c.cs, element...)}
}

type lpushx struct {
	cs []string
}

func (c lpushx) Key(key string) lpushxKey {
	return lpushxKey{cs: append(c.cs, key)}
}

func (b *Builder) Lpushx() (c lpushx) {
	c.cs = append(b.get(), "LPUSHX")
	return
}

type lpushxElement struct {
	cs []string
}

func (c lpushxElement) Element(element ...string) lpushxElement {
	return lpushxElement{cs: append(c.cs, element...)}
}

func (c lpushxElement) Build() []string {
	return c.cs
}

type lpushxKey struct {
	cs []string
}

func (c lpushxKey) Element(element ...string) lpushxElement {
	return lpushxElement{cs: append(c.cs, element...)}
}

type lrange struct {
	cs []string
}

func (c lrange) Key(key string) lrangeKey {
	return lrangeKey{cs: append(c.cs, key)}
}

func (b *Builder) Lrange() (c lrange) {
	c.cs = append(b.get(), "LRANGE")
	return
}

type lrangeKey struct {
	cs []string
}

func (c lrangeKey) Start(start int64) lrangeStart {
	return lrangeStart{cs: append(c.cs, strconv.FormatInt(start, 10))}
}

type lrangeStart struct {
	cs []string
}

func (c lrangeStart) Stop(stop int64) lrangeStop {
	return lrangeStop{cs: append(c.cs, strconv.FormatInt(stop, 10))}
}

type lrangeStop struct {
	cs []string
}

func (c lrangeStop) Build() []string {
	return c.cs
}

type lrem struct {
	cs []string
}

func (c lrem) Key(key string) lremKey {
	return lremKey{cs: append(c.cs, key)}
}

func (b *Builder) Lrem() (c lrem) {
	c.cs = append(b.get(), "LREM")
	return
}

type lremCount struct {
	cs []string
}

func (c lremCount) Element(element string) lremElement {
	return lremElement{cs: append(c.cs, element)}
}

type lremElement struct {
	cs []string
}

func (c lremElement) Build() []string {
	return c.cs
}

type lremKey struct {
	cs []string
}

func (c lremKey) Count(count int64) lremCount {
	return lremCount{cs: append(c.cs, strconv.FormatInt(count, 10))}
}

type lset struct {
	cs []string
}

func (c lset) Key(key string) lsetKey {
	return lsetKey{cs: append(c.cs, key)}
}

func (b *Builder) Lset() (c lset) {
	c.cs = append(b.get(), "LSET")
	return
}

type lsetElement struct {
	cs []string
}

func (c lsetElement) Build() []string {
	return c.cs
}

type lsetIndex struct {
	cs []string
}

func (c lsetIndex) Element(element string) lsetElement {
	return lsetElement{cs: append(c.cs, element)}
}

type lsetKey struct {
	cs []string
}

func (c lsetKey) Index(index int64) lsetIndex {
	return lsetIndex{cs: append(c.cs, strconv.FormatInt(index, 10))}
}

type ltrim struct {
	cs []string
}

func (c ltrim) Key(key string) ltrimKey {
	return ltrimKey{cs: append(c.cs, key)}
}

func (b *Builder) Ltrim() (c ltrim) {
	c.cs = append(b.get(), "LTRIM")
	return
}

type ltrimKey struct {
	cs []string
}

func (c ltrimKey) Start(start int64) ltrimStart {
	return ltrimStart{cs: append(c.cs, strconv.FormatInt(start, 10))}
}

type ltrimStart struct {
	cs []string
}

func (c ltrimStart) Stop(stop int64) ltrimStop {
	return ltrimStop{cs: append(c.cs, strconv.FormatInt(stop, 10))}
}

type ltrimStop struct {
	cs []string
}

func (c ltrimStop) Build() []string {
	return c.cs
}

type memoryDoctor struct {
	cs []string
}

func (c memoryDoctor) Build() []string {
	return c.cs
}

func (b *Builder) MemoryDoctor() (c memoryDoctor) {
	c.cs = append(b.get(), "MEMORY", "DOCTOR")
	return
}

type memoryHelp struct {
	cs []string
}

func (c memoryHelp) Build() []string {
	return c.cs
}

func (b *Builder) MemoryHelp() (c memoryHelp) {
	c.cs = append(b.get(), "MEMORY", "HELP")
	return
}

type memoryMallocStats struct {
	cs []string
}

func (c memoryMallocStats) Build() []string {
	return c.cs
}

func (b *Builder) MemoryMallocStats() (c memoryMallocStats) {
	c.cs = append(b.get(), "MEMORY", "MALLOC-STATS")
	return
}

type memoryPurge struct {
	cs []string
}

func (c memoryPurge) Build() []string {
	return c.cs
}

func (b *Builder) MemoryPurge() (c memoryPurge) {
	c.cs = append(b.get(), "MEMORY", "PURGE")
	return
}

type memoryStats struct {
	cs []string
}

func (c memoryStats) Build() []string {
	return c.cs
}

func (b *Builder) MemoryStats() (c memoryStats) {
	c.cs = append(b.get(), "MEMORY", "STATS")
	return
}

type memoryUsage struct {
	cs []string
}

func (c memoryUsage) Key(key string) memoryUsageKey {
	return memoryUsageKey{cs: append(c.cs, key)}
}

func (b *Builder) MemoryUsage() (c memoryUsage) {
	c.cs = append(b.get(), "MEMORY", "USAGE")
	return
}

type memoryUsageKey struct {
	cs []string
}

func (c memoryUsageKey) Samples(count int64) memoryUsageSamples {
	return memoryUsageSamples{cs: append(c.cs, "SAMPLES", strconv.FormatInt(count, 10))}
}

func (c memoryUsageKey) Build() []string {
	return c.cs
}

type memoryUsageSamples struct {
	cs []string
}

func (c memoryUsageSamples) Build() []string {
	return c.cs
}

type mget struct {
	cs []string
}

func (c mget) Key(key ...string) mgetKey {
	return mgetKey{cs: append(c.cs, key...)}
}

func (b *Builder) Mget() (c mget) {
	c.cs = append(b.get(), "MGET")
	return
}

type mgetKey struct {
	cs []string
}

func (c mgetKey) Key(key ...string) mgetKey {
	return mgetKey{cs: append(c.cs, key...)}
}

func (c mgetKey) Build() []string {
	return c.cs
}

type migrate struct {
	cs []string
}

func (c migrate) Host(host string) migrateHost {
	return migrateHost{cs: append(c.cs, host)}
}

func (b *Builder) Migrate() (c migrate) {
	c.cs = append(b.get(), "MIGRATE")
	return
}

type migrateAuth struct {
	cs []string
}

func (c migrateAuth) Auth2(usernamePassword string) migrateAuth2 {
	return migrateAuth2{cs: append(c.cs, "AUTH2", usernamePassword)}
}

func (c migrateAuth) Keys(key ...string) migrateKeys {
	c.cs = append(c.cs, "KEYS")
	return migrateKeys{cs: append(c.cs, key...)}
}

func (c migrateAuth) Build() []string {
	return c.cs
}

type migrateAuth2 struct {
	cs []string
}

func (c migrateAuth2) Keys(key ...string) migrateKeys {
	c.cs = append(c.cs, "KEYS")
	return migrateKeys{cs: append(c.cs, key...)}
}

func (c migrateAuth2) Build() []string {
	return c.cs
}

type migrateCopyCopy struct {
	cs []string
}

func (c migrateCopyCopy) Replace() migrateReplaceReplace {
	return migrateReplaceReplace{cs: append(c.cs, "REPLACE")}
}

func (c migrateCopyCopy) Auth(password string) migrateAuth {
	return migrateAuth{cs: append(c.cs, "AUTH", password)}
}

func (c migrateCopyCopy) Auth2(usernamePassword string) migrateAuth2 {
	return migrateAuth2{cs: append(c.cs, "AUTH2", usernamePassword)}
}

func (c migrateCopyCopy) Keys(key ...string) migrateKeys {
	c.cs = append(c.cs, "KEYS")
	return migrateKeys{cs: append(c.cs, key...)}
}

func (c migrateCopyCopy) Build() []string {
	return c.cs
}

type migrateDestinationDb struct {
	cs []string
}

func (c migrateDestinationDb) Timeout(timeout int64) migrateTimeout {
	return migrateTimeout{cs: append(c.cs, strconv.FormatInt(timeout, 10))}
}

type migrateHost struct {
	cs []string
}

func (c migrateHost) Port(port string) migratePort {
	return migratePort{cs: append(c.cs, port)}
}

type migrateKeyEmpty struct {
	cs []string
}

func (c migrateKeyEmpty) DestinationDb(destinationDb int64) migrateDestinationDb {
	return migrateDestinationDb{cs: append(c.cs, strconv.FormatInt(destinationDb, 10))}
}

type migrateKeyKey struct {
	cs []string
}

func (c migrateKeyKey) DestinationDb(destinationDb int64) migrateDestinationDb {
	return migrateDestinationDb{cs: append(c.cs, strconv.FormatInt(destinationDb, 10))}
}

type migrateKeys struct {
	cs []string
}

func (c migrateKeys) Keys(keys ...string) migrateKeys {
	return migrateKeys{cs: append(c.cs, keys...)}
}

func (c migrateKeys) Build() []string {
	return c.cs
}

type migratePort struct {
	cs []string
}

func (c migratePort) Key() migrateKeyKey {
	return migrateKeyKey{cs: append(c.cs, "key")}
}

func (c migratePort) Empty() migrateKeyEmpty {
	return migrateKeyEmpty{cs: append(c.cs, "\"\"")}
}

type migrateReplaceReplace struct {
	cs []string
}

func (c migrateReplaceReplace) Auth(password string) migrateAuth {
	return migrateAuth{cs: append(c.cs, "AUTH", password)}
}

func (c migrateReplaceReplace) Auth2(usernamePassword string) migrateAuth2 {
	return migrateAuth2{cs: append(c.cs, "AUTH2", usernamePassword)}
}

func (c migrateReplaceReplace) Keys(key ...string) migrateKeys {
	c.cs = append(c.cs, "KEYS")
	return migrateKeys{cs: append(c.cs, key...)}
}

func (c migrateReplaceReplace) Build() []string {
	return c.cs
}

type migrateTimeout struct {
	cs []string
}

func (c migrateTimeout) Copy() migrateCopyCopy {
	return migrateCopyCopy{cs: append(c.cs, "COPY")}
}

func (c migrateTimeout) Replace() migrateReplaceReplace {
	return migrateReplaceReplace{cs: append(c.cs, "REPLACE")}
}

func (c migrateTimeout) Auth(password string) migrateAuth {
	return migrateAuth{cs: append(c.cs, "AUTH", password)}
}

func (c migrateTimeout) Auth2(usernamePassword string) migrateAuth2 {
	return migrateAuth2{cs: append(c.cs, "AUTH2", usernamePassword)}
}

func (c migrateTimeout) Keys(key ...string) migrateKeys {
	c.cs = append(c.cs, "KEYS")
	return migrateKeys{cs: append(c.cs, key...)}
}

func (c migrateTimeout) Build() []string {
	return c.cs
}

type moduleList struct {
	cs []string
}

func (c moduleList) Build() []string {
	return c.cs
}

func (b *Builder) ModuleList() (c moduleList) {
	c.cs = append(b.get(), "MODULE", "LIST")
	return
}

type moduleLoad struct {
	cs []string
}

func (c moduleLoad) Path(path string) moduleLoadPath {
	return moduleLoadPath{cs: append(c.cs, path)}
}

func (b *Builder) ModuleLoad() (c moduleLoad) {
	c.cs = append(b.get(), "MODULE", "LOAD")
	return
}

type moduleLoadArg struct {
	cs []string
}

func (c moduleLoadArg) Arg(arg ...string) moduleLoadArg {
	return moduleLoadArg{cs: append(c.cs, arg...)}
}

func (c moduleLoadArg) Build() []string {
	return c.cs
}

type moduleLoadPath struct {
	cs []string
}

func (c moduleLoadPath) Arg(arg ...string) moduleLoadArg {
	return moduleLoadArg{cs: append(c.cs, arg...)}
}

func (c moduleLoadPath) Build() []string {
	return c.cs
}

type moduleUnload struct {
	cs []string
}

func (c moduleUnload) Name(name string) moduleUnloadName {
	return moduleUnloadName{cs: append(c.cs, name)}
}

func (b *Builder) ModuleUnload() (c moduleUnload) {
	c.cs = append(b.get(), "MODULE", "UNLOAD")
	return
}

type moduleUnloadName struct {
	cs []string
}

func (c moduleUnloadName) Build() []string {
	return c.cs
}

type monitor struct {
	cs []string
}

func (c monitor) Build() []string {
	return c.cs
}

func (b *Builder) Monitor() (c monitor) {
	c.cs = append(b.get(), "MONITOR")
	return
}

type move struct {
	cs []string
}

func (c move) Key(key string) moveKey {
	return moveKey{cs: append(c.cs, key)}
}

func (b *Builder) Move() (c move) {
	c.cs = append(b.get(), "MOVE")
	return
}

type moveDb struct {
	cs []string
}

func (c moveDb) Build() []string {
	return c.cs
}

type moveKey struct {
	cs []string
}

func (c moveKey) Db(db int64) moveDb {
	return moveDb{cs: append(c.cs, strconv.FormatInt(db, 10))}
}

type mset struct {
	cs []string
}

func (c mset) KeyValue() msetKeyValue {
	return msetKeyValue{cs: append(c.cs)}
}

func (b *Builder) Mset() (c mset) {
	c.cs = append(b.get(), "MSET")
	return
}

type msetKeyValue struct {
	cs []string
}

func (c msetKeyValue) KeyValue(key string, value string) msetKeyValue {
	return msetKeyValue{cs: append(c.cs, key, value)}
}

func (c msetKeyValue) Build() []string {
	return c.cs
}

type msetnx struct {
	cs []string
}

func (c msetnx) KeyValue() msetnxKeyValue {
	return msetnxKeyValue{cs: append(c.cs)}
}

func (b *Builder) Msetnx() (c msetnx) {
	c.cs = append(b.get(), "MSETNX")
	return
}

type msetnxKeyValue struct {
	cs []string
}

func (c msetnxKeyValue) KeyValue(key string, value string) msetnxKeyValue {
	return msetnxKeyValue{cs: append(c.cs, key, value)}
}

func (c msetnxKeyValue) Build() []string {
	return c.cs
}

type multi struct {
	cs []string
}

func (c multi) Build() []string {
	return c.cs
}

func (b *Builder) Multi() (c multi) {
	c.cs = append(b.get(), "MULTI")
	return
}

type object struct {
	cs []string
}

func (c object) Subcommand(subcommand string) objectSubcommand {
	return objectSubcommand{cs: append(c.cs, subcommand)}
}

func (b *Builder) Object() (c object) {
	c.cs = append(b.get(), "OBJECT")
	return
}

type objectArguments struct {
	cs []string
}

func (c objectArguments) Arguments(arguments ...string) objectArguments {
	return objectArguments{cs: append(c.cs, arguments...)}
}

func (c objectArguments) Build() []string {
	return c.cs
}

type objectSubcommand struct {
	cs []string
}

func (c objectSubcommand) Arguments(arguments ...string) objectArguments {
	return objectArguments{cs: append(c.cs, arguments...)}
}

func (c objectSubcommand) Build() []string {
	return c.cs
}

type persist struct {
	cs []string
}

func (c persist) Key(key string) persistKey {
	return persistKey{cs: append(c.cs, key)}
}

func (b *Builder) Persist() (c persist) {
	c.cs = append(b.get(), "PERSIST")
	return
}

type persistKey struct {
	cs []string
}

func (c persistKey) Build() []string {
	return c.cs
}

type pexpire struct {
	cs []string
}

func (c pexpire) Key(key string) pexpireKey {
	return pexpireKey{cs: append(c.cs, key)}
}

func (b *Builder) Pexpire() (c pexpire) {
	c.cs = append(b.get(), "PEXPIRE")
	return
}

type pexpireConditionGt struct {
	cs []string
}

func (c pexpireConditionGt) Build() []string {
	return c.cs
}

type pexpireConditionLt struct {
	cs []string
}

func (c pexpireConditionLt) Build() []string {
	return c.cs
}

type pexpireConditionNx struct {
	cs []string
}

func (c pexpireConditionNx) Build() []string {
	return c.cs
}

type pexpireConditionXx struct {
	cs []string
}

func (c pexpireConditionXx) Build() []string {
	return c.cs
}

type pexpireKey struct {
	cs []string
}

func (c pexpireKey) Milliseconds(milliseconds int64) pexpireMilliseconds {
	return pexpireMilliseconds{cs: append(c.cs, strconv.FormatInt(milliseconds, 10))}
}

type pexpireMilliseconds struct {
	cs []string
}

func (c pexpireMilliseconds) Nx() pexpireConditionNx {
	return pexpireConditionNx{cs: append(c.cs, "NX")}
}

func (c pexpireMilliseconds) Xx() pexpireConditionXx {
	return pexpireConditionXx{cs: append(c.cs, "XX")}
}

func (c pexpireMilliseconds) Gt() pexpireConditionGt {
	return pexpireConditionGt{cs: append(c.cs, "GT")}
}

func (c pexpireMilliseconds) Lt() pexpireConditionLt {
	return pexpireConditionLt{cs: append(c.cs, "LT")}
}

func (c pexpireMilliseconds) Build() []string {
	return c.cs
}

type pexpireat struct {
	cs []string
}

func (c pexpireat) Key(key string) pexpireatKey {
	return pexpireatKey{cs: append(c.cs, key)}
}

func (b *Builder) Pexpireat() (c pexpireat) {
	c.cs = append(b.get(), "PEXPIREAT")
	return
}

type pexpireatConditionGt struct {
	cs []string
}

func (c pexpireatConditionGt) Build() []string {
	return c.cs
}

type pexpireatConditionLt struct {
	cs []string
}

func (c pexpireatConditionLt) Build() []string {
	return c.cs
}

type pexpireatConditionNx struct {
	cs []string
}

func (c pexpireatConditionNx) Build() []string {
	return c.cs
}

type pexpireatConditionXx struct {
	cs []string
}

func (c pexpireatConditionXx) Build() []string {
	return c.cs
}

type pexpireatKey struct {
	cs []string
}

func (c pexpireatKey) MillisecondsTimestamp(millisecondsTimestamp int64) pexpireatMillisecondsTimestamp {
	return pexpireatMillisecondsTimestamp{cs: append(c.cs, strconv.FormatInt(millisecondsTimestamp, 10))}
}

type pexpireatMillisecondsTimestamp struct {
	cs []string
}

func (c pexpireatMillisecondsTimestamp) Nx() pexpireatConditionNx {
	return pexpireatConditionNx{cs: append(c.cs, "NX")}
}

func (c pexpireatMillisecondsTimestamp) Xx() pexpireatConditionXx {
	return pexpireatConditionXx{cs: append(c.cs, "XX")}
}

func (c pexpireatMillisecondsTimestamp) Gt() pexpireatConditionGt {
	return pexpireatConditionGt{cs: append(c.cs, "GT")}
}

func (c pexpireatMillisecondsTimestamp) Lt() pexpireatConditionLt {
	return pexpireatConditionLt{cs: append(c.cs, "LT")}
}

func (c pexpireatMillisecondsTimestamp) Build() []string {
	return c.cs
}

type pexpiretime struct {
	cs []string
}

func (c pexpiretime) Key(key string) pexpiretimeKey {
	return pexpiretimeKey{cs: append(c.cs, key)}
}

func (b *Builder) Pexpiretime() (c pexpiretime) {
	c.cs = append(b.get(), "PEXPIRETIME")
	return
}

type pexpiretimeKey struct {
	cs []string
}

func (c pexpiretimeKey) Build() []string {
	return c.cs
}

type pfadd struct {
	cs []string
}

func (c pfadd) Key(key string) pfaddKey {
	return pfaddKey{cs: append(c.cs, key)}
}

func (b *Builder) Pfadd() (c pfadd) {
	c.cs = append(b.get(), "PFADD")
	return
}

type pfaddElement struct {
	cs []string
}

func (c pfaddElement) Element(element ...string) pfaddElement {
	return pfaddElement{cs: append(c.cs, element...)}
}

func (c pfaddElement) Build() []string {
	return c.cs
}

type pfaddKey struct {
	cs []string
}

func (c pfaddKey) Element(element ...string) pfaddElement {
	return pfaddElement{cs: append(c.cs, element...)}
}

func (c pfaddKey) Build() []string {
	return c.cs
}

type pfcount struct {
	cs []string
}

func (c pfcount) Key(key ...string) pfcountKey {
	return pfcountKey{cs: append(c.cs, key...)}
}

func (b *Builder) Pfcount() (c pfcount) {
	c.cs = append(b.get(), "PFCOUNT")
	return
}

type pfcountKey struct {
	cs []string
}

func (c pfcountKey) Key(key ...string) pfcountKey {
	return pfcountKey{cs: append(c.cs, key...)}
}

func (c pfcountKey) Build() []string {
	return c.cs
}

type pfmerge struct {
	cs []string
}

func (c pfmerge) Destkey(destkey string) pfmergeDestkey {
	return pfmergeDestkey{cs: append(c.cs, destkey)}
}

func (b *Builder) Pfmerge() (c pfmerge) {
	c.cs = append(b.get(), "PFMERGE")
	return
}

type pfmergeDestkey struct {
	cs []string
}

func (c pfmergeDestkey) Sourcekey(sourcekey ...string) pfmergeSourcekey {
	return pfmergeSourcekey{cs: append(c.cs, sourcekey...)}
}

type pfmergeSourcekey struct {
	cs []string
}

func (c pfmergeSourcekey) Sourcekey(sourcekey ...string) pfmergeSourcekey {
	return pfmergeSourcekey{cs: append(c.cs, sourcekey...)}
}

func (c pfmergeSourcekey) Build() []string {
	return c.cs
}

type ping struct {
	cs []string
}

func (c ping) Message(message string) pingMessage {
	return pingMessage{cs: append(c.cs, message)}
}

func (c ping) Build() []string {
	return c.cs
}

func (b *Builder) Ping() (c ping) {
	c.cs = append(b.get(), "PING")
	return
}

type pingMessage struct {
	cs []string
}

func (c pingMessage) Build() []string {
	return c.cs
}

type psetex struct {
	cs []string
}

func (c psetex) Key(key string) psetexKey {
	return psetexKey{cs: append(c.cs, key)}
}

func (b *Builder) Psetex() (c psetex) {
	c.cs = append(b.get(), "PSETEX")
	return
}

type psetexKey struct {
	cs []string
}

func (c psetexKey) Milliseconds(milliseconds int64) psetexMilliseconds {
	return psetexMilliseconds{cs: append(c.cs, strconv.FormatInt(milliseconds, 10))}
}

type psetexMilliseconds struct {
	cs []string
}

func (c psetexMilliseconds) Value(value string) psetexValue {
	return psetexValue{cs: append(c.cs, value)}
}

type psetexValue struct {
	cs []string
}

func (c psetexValue) Build() []string {
	return c.cs
}

type psubscribe struct {
	cs []string
}

func (c psubscribe) Pattern(pattern ...string) psubscribePattern {
	return psubscribePattern{cs: append(c.cs, pattern...)}
}

func (b *Builder) Psubscribe() (c psubscribe) {
	c.cs = append(b.get(), "PSUBSCRIBE")
	return
}

type psubscribePattern struct {
	cs []string
}

func (c psubscribePattern) Pattern(pattern ...string) psubscribePattern {
	return psubscribePattern{cs: append(c.cs, pattern...)}
}

func (c psubscribePattern) Build() []string {
	return c.cs
}

type psync struct {
	cs []string
}

func (c psync) Replicationid(replicationid int64) psyncReplicationid {
	return psyncReplicationid{cs: append(c.cs, strconv.FormatInt(replicationid, 10))}
}

func (b *Builder) Psync() (c psync) {
	c.cs = append(b.get(), "PSYNC")
	return
}

type psyncOffset struct {
	cs []string
}

func (c psyncOffset) Build() []string {
	return c.cs
}

type psyncReplicationid struct {
	cs []string
}

func (c psyncReplicationid) Offset(offset int64) psyncOffset {
	return psyncOffset{cs: append(c.cs, strconv.FormatInt(offset, 10))}
}

type pttl struct {
	cs []string
}

func (c pttl) Key(key string) pttlKey {
	return pttlKey{cs: append(c.cs, key)}
}

func (b *Builder) Pttl() (c pttl) {
	c.cs = append(b.get(), "PTTL")
	return
}

type pttlKey struct {
	cs []string
}

func (c pttlKey) Build() []string {
	return c.cs
}

type publish struct {
	cs []string
}

func (c publish) Channel(channel string) publishChannel {
	return publishChannel{cs: append(c.cs, channel)}
}

func (b *Builder) Publish() (c publish) {
	c.cs = append(b.get(), "PUBLISH")
	return
}

type publishChannel struct {
	cs []string
}

func (c publishChannel) Message(message string) publishMessage {
	return publishMessage{cs: append(c.cs, message)}
}

type publishMessage struct {
	cs []string
}

func (c publishMessage) Build() []string {
	return c.cs
}

type pubsub struct {
	cs []string
}

func (c pubsub) Subcommand(subcommand string) pubsubSubcommand {
	return pubsubSubcommand{cs: append(c.cs, subcommand)}
}

func (b *Builder) Pubsub() (c pubsub) {
	c.cs = append(b.get(), "PUBSUB")
	return
}

type pubsubArgument struct {
	cs []string
}

func (c pubsubArgument) Argument(argument ...string) pubsubArgument {
	return pubsubArgument{cs: append(c.cs, argument...)}
}

func (c pubsubArgument) Build() []string {
	return c.cs
}

type pubsubSubcommand struct {
	cs []string
}

func (c pubsubSubcommand) Argument(argument ...string) pubsubArgument {
	return pubsubArgument{cs: append(c.cs, argument...)}
}

func (c pubsubSubcommand) Build() []string {
	return c.cs
}

type punsubscribe struct {
	cs []string
}

func (c punsubscribe) Pattern(pattern ...string) punsubscribePattern {
	return punsubscribePattern{cs: append(c.cs, pattern...)}
}

func (c punsubscribe) Build() []string {
	return c.cs
}

func (b *Builder) Punsubscribe() (c punsubscribe) {
	c.cs = append(b.get(), "PUNSUBSCRIBE")
	return
}

type punsubscribePattern struct {
	cs []string
}

func (c punsubscribePattern) Pattern(pattern ...string) punsubscribePattern {
	return punsubscribePattern{cs: append(c.cs, pattern...)}
}

func (c punsubscribePattern) Build() []string {
	return c.cs
}

type quit struct {
	cs []string
}

func (c quit) Build() []string {
	return c.cs
}

func (b *Builder) Quit() (c quit) {
	c.cs = append(b.get(), "QUIT")
	return
}

type randomkey struct {
	cs []string
}

func (c randomkey) Build() []string {
	return c.cs
}

func (b *Builder) Randomkey() (c randomkey) {
	c.cs = append(b.get(), "RANDOMKEY")
	return
}

type readonly struct {
	cs []string
}

func (c readonly) Build() []string {
	return c.cs
}

func (b *Builder) Readonly() (c readonly) {
	c.cs = append(b.get(), "READONLY")
	return
}

type readwrite struct {
	cs []string
}

func (c readwrite) Build() []string {
	return c.cs
}

func (b *Builder) Readwrite() (c readwrite) {
	c.cs = append(b.get(), "READWRITE")
	return
}

type rename struct {
	cs []string
}

func (c rename) Key(key string) renameKey {
	return renameKey{cs: append(c.cs, key)}
}

func (b *Builder) Rename() (c rename) {
	c.cs = append(b.get(), "RENAME")
	return
}

type renameKey struct {
	cs []string
}

func (c renameKey) Newkey(newkey string) renameNewkey {
	return renameNewkey{cs: append(c.cs, newkey)}
}

type renameNewkey struct {
	cs []string
}

func (c renameNewkey) Build() []string {
	return c.cs
}

type renamenx struct {
	cs []string
}

func (c renamenx) Key(key string) renamenxKey {
	return renamenxKey{cs: append(c.cs, key)}
}

func (b *Builder) Renamenx() (c renamenx) {
	c.cs = append(b.get(), "RENAMENX")
	return
}

type renamenxKey struct {
	cs []string
}

func (c renamenxKey) Newkey(newkey string) renamenxNewkey {
	return renamenxNewkey{cs: append(c.cs, newkey)}
}

type renamenxNewkey struct {
	cs []string
}

func (c renamenxNewkey) Build() []string {
	return c.cs
}

type replicaof struct {
	cs []string
}

func (c replicaof) Host(host string) replicaofHost {
	return replicaofHost{cs: append(c.cs, host)}
}

func (b *Builder) Replicaof() (c replicaof) {
	c.cs = append(b.get(), "REPLICAOF")
	return
}

type replicaofHost struct {
	cs []string
}

func (c replicaofHost) Port(port string) replicaofPort {
	return replicaofPort{cs: append(c.cs, port)}
}

type replicaofPort struct {
	cs []string
}

func (c replicaofPort) Build() []string {
	return c.cs
}

type reset struct {
	cs []string
}

func (c reset) Build() []string {
	return c.cs
}

func (b *Builder) Reset() (c reset) {
	c.cs = append(b.get(), "RESET")
	return
}

type restore struct {
	cs []string
}

func (c restore) Key(key string) restoreKey {
	return restoreKey{cs: append(c.cs, key)}
}

func (b *Builder) Restore() (c restore) {
	c.cs = append(b.get(), "RESTORE")
	return
}

type restoreAbsttlAbsttl struct {
	cs []string
}

func (c restoreAbsttlAbsttl) Idletime(seconds int64) restoreIdletime {
	return restoreIdletime{cs: append(c.cs, "IDLETIME", strconv.FormatInt(seconds, 10))}
}

func (c restoreAbsttlAbsttl) Freq(frequency int64) restoreFreq {
	return restoreFreq{cs: append(c.cs, "FREQ", strconv.FormatInt(frequency, 10))}
}

func (c restoreAbsttlAbsttl) Build() []string {
	return c.cs
}

type restoreFreq struct {
	cs []string
}

func (c restoreFreq) Build() []string {
	return c.cs
}

type restoreIdletime struct {
	cs []string
}

func (c restoreIdletime) Freq(frequency int64) restoreFreq {
	return restoreFreq{cs: append(c.cs, "FREQ", strconv.FormatInt(frequency, 10))}
}

func (c restoreIdletime) Build() []string {
	return c.cs
}

type restoreKey struct {
	cs []string
}

func (c restoreKey) Ttl(ttl int64) restoreTtl {
	return restoreTtl{cs: append(c.cs, strconv.FormatInt(ttl, 10))}
}

type restoreReplaceReplace struct {
	cs []string
}

func (c restoreReplaceReplace) Absttl() restoreAbsttlAbsttl {
	return restoreAbsttlAbsttl{cs: append(c.cs, "ABSTTL")}
}

func (c restoreReplaceReplace) Idletime(seconds int64) restoreIdletime {
	return restoreIdletime{cs: append(c.cs, "IDLETIME", strconv.FormatInt(seconds, 10))}
}

func (c restoreReplaceReplace) Freq(frequency int64) restoreFreq {
	return restoreFreq{cs: append(c.cs, "FREQ", strconv.FormatInt(frequency, 10))}
}

func (c restoreReplaceReplace) Build() []string {
	return c.cs
}

type restoreSerializedValue struct {
	cs []string
}

func (c restoreSerializedValue) Replace() restoreReplaceReplace {
	return restoreReplaceReplace{cs: append(c.cs, "REPLACE")}
}

func (c restoreSerializedValue) Absttl() restoreAbsttlAbsttl {
	return restoreAbsttlAbsttl{cs: append(c.cs, "ABSTTL")}
}

func (c restoreSerializedValue) Idletime(seconds int64) restoreIdletime {
	return restoreIdletime{cs: append(c.cs, "IDLETIME", strconv.FormatInt(seconds, 10))}
}

func (c restoreSerializedValue) Freq(frequency int64) restoreFreq {
	return restoreFreq{cs: append(c.cs, "FREQ", strconv.FormatInt(frequency, 10))}
}

func (c restoreSerializedValue) Build() []string {
	return c.cs
}

type restoreTtl struct {
	cs []string
}

func (c restoreTtl) SerializedValue(serializedValue string) restoreSerializedValue {
	return restoreSerializedValue{cs: append(c.cs, serializedValue)}
}

type role struct {
	cs []string
}

func (c role) Build() []string {
	return c.cs
}

func (b *Builder) Role() (c role) {
	c.cs = append(b.get(), "ROLE")
	return
}

type rpop struct {
	cs []string
}

func (c rpop) Key(key string) rpopKey {
	return rpopKey{cs: append(c.cs, key)}
}

func (b *Builder) Rpop() (c rpop) {
	c.cs = append(b.get(), "RPOP")
	return
}

type rpopCount struct {
	cs []string
}

func (c rpopCount) Build() []string {
	return c.cs
}

type rpopKey struct {
	cs []string
}

func (c rpopKey) Count(count int64) rpopCount {
	return rpopCount{cs: append(c.cs, strconv.FormatInt(count, 10))}
}

func (c rpopKey) Build() []string {
	return c.cs
}

type rpoplpush struct {
	cs []string
}

func (c rpoplpush) Source(source string) rpoplpushSource {
	return rpoplpushSource{cs: append(c.cs, source)}
}

func (b *Builder) Rpoplpush() (c rpoplpush) {
	c.cs = append(b.get(), "RPOPLPUSH")
	return
}

type rpoplpushDestination struct {
	cs []string
}

func (c rpoplpushDestination) Build() []string {
	return c.cs
}

type rpoplpushSource struct {
	cs []string
}

func (c rpoplpushSource) Destination(destination string) rpoplpushDestination {
	return rpoplpushDestination{cs: append(c.cs, destination)}
}

type rpush struct {
	cs []string
}

func (c rpush) Key(key string) rpushKey {
	return rpushKey{cs: append(c.cs, key)}
}

func (b *Builder) Rpush() (c rpush) {
	c.cs = append(b.get(), "RPUSH")
	return
}

type rpushElement struct {
	cs []string
}

func (c rpushElement) Element(element ...string) rpushElement {
	return rpushElement{cs: append(c.cs, element...)}
}

func (c rpushElement) Build() []string {
	return c.cs
}

type rpushKey struct {
	cs []string
}

func (c rpushKey) Element(element ...string) rpushElement {
	return rpushElement{cs: append(c.cs, element...)}
}

type rpushx struct {
	cs []string
}

func (c rpushx) Key(key string) rpushxKey {
	return rpushxKey{cs: append(c.cs, key)}
}

func (b *Builder) Rpushx() (c rpushx) {
	c.cs = append(b.get(), "RPUSHX")
	return
}

type rpushxElement struct {
	cs []string
}

func (c rpushxElement) Element(element ...string) rpushxElement {
	return rpushxElement{cs: append(c.cs, element...)}
}

func (c rpushxElement) Build() []string {
	return c.cs
}

type rpushxKey struct {
	cs []string
}

func (c rpushxKey) Element(element ...string) rpushxElement {
	return rpushxElement{cs: append(c.cs, element...)}
}

type sadd struct {
	cs []string
}

func (c sadd) Key(key string) saddKey {
	return saddKey{cs: append(c.cs, key)}
}

func (b *Builder) Sadd() (c sadd) {
	c.cs = append(b.get(), "SADD")
	return
}

type saddKey struct {
	cs []string
}

func (c saddKey) Member(member ...string) saddMember {
	return saddMember{cs: append(c.cs, member...)}
}

type saddMember struct {
	cs []string
}

func (c saddMember) Member(member ...string) saddMember {
	return saddMember{cs: append(c.cs, member...)}
}

func (c saddMember) Build() []string {
	return c.cs
}

type save struct {
	cs []string
}

func (c save) Build() []string {
	return c.cs
}

func (b *Builder) Save() (c save) {
	c.cs = append(b.get(), "SAVE")
	return
}

type scan struct {
	cs []string
}

func (c scan) Cursor(cursor int64) scanCursor {
	return scanCursor{cs: append(c.cs, strconv.FormatInt(cursor, 10))}
}

func (b *Builder) Scan() (c scan) {
	c.cs = append(b.get(), "SCAN")
	return
}

type scanCount struct {
	cs []string
}

func (c scanCount) Type(typ string) scanType {
	return scanType{cs: append(c.cs, "TYPE", typ)}
}

func (c scanCount) Build() []string {
	return c.cs
}

type scanCursor struct {
	cs []string
}

func (c scanCursor) Match(pattern string) scanMatch {
	return scanMatch{cs: append(c.cs, "MATCH", pattern)}
}

func (c scanCursor) Count(count int64) scanCount {
	return scanCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c scanCursor) Type(typ string) scanType {
	return scanType{cs: append(c.cs, "TYPE", typ)}
}

func (c scanCursor) Build() []string {
	return c.cs
}

type scanMatch struct {
	cs []string
}

func (c scanMatch) Count(count int64) scanCount {
	return scanCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c scanMatch) Type(typ string) scanType {
	return scanType{cs: append(c.cs, "TYPE", typ)}
}

func (c scanMatch) Build() []string {
	return c.cs
}

type scanType struct {
	cs []string
}

func (c scanType) Build() []string {
	return c.cs
}

type scard struct {
	cs []string
}

func (c scard) Key(key string) scardKey {
	return scardKey{cs: append(c.cs, key)}
}

func (b *Builder) Scard() (c scard) {
	c.cs = append(b.get(), "SCARD")
	return
}

type scardKey struct {
	cs []string
}

func (c scardKey) Build() []string {
	return c.cs
}

type scriptDebug struct {
	cs []string
}

func (c scriptDebug) Yes() scriptDebugModeYes {
	return scriptDebugModeYes{cs: append(c.cs, "YES")}
}

func (c scriptDebug) Sync() scriptDebugModeSync {
	return scriptDebugModeSync{cs: append(c.cs, "SYNC")}
}

func (c scriptDebug) No() scriptDebugModeNo {
	return scriptDebugModeNo{cs: append(c.cs, "NO")}
}

func (b *Builder) ScriptDebug() (c scriptDebug) {
	c.cs = append(b.get(), "SCRIPT", "DEBUG")
	return
}

type scriptDebugModeNo struct {
	cs []string
}

func (c scriptDebugModeNo) Build() []string {
	return c.cs
}

type scriptDebugModeSync struct {
	cs []string
}

func (c scriptDebugModeSync) Build() []string {
	return c.cs
}

type scriptDebugModeYes struct {
	cs []string
}

func (c scriptDebugModeYes) Build() []string {
	return c.cs
}

type scriptExists struct {
	cs []string
}

func (c scriptExists) Sha1(sha1 ...string) scriptExistsSha1 {
	return scriptExistsSha1{cs: append(c.cs, sha1...)}
}

func (b *Builder) ScriptExists() (c scriptExists) {
	c.cs = append(b.get(), "SCRIPT", "EXISTS")
	return
}

type scriptExistsSha1 struct {
	cs []string
}

func (c scriptExistsSha1) Sha1(sha1 ...string) scriptExistsSha1 {
	return scriptExistsSha1{cs: append(c.cs, sha1...)}
}

func (c scriptExistsSha1) Build() []string {
	return c.cs
}

type scriptFlush struct {
	cs []string
}

func (c scriptFlush) Async() scriptFlushAsyncAsync {
	return scriptFlushAsyncAsync{cs: append(c.cs, "ASYNC")}
}

func (c scriptFlush) Sync() scriptFlushAsyncSync {
	return scriptFlushAsyncSync{cs: append(c.cs, "SYNC")}
}

func (c scriptFlush) Build() []string {
	return c.cs
}

func (b *Builder) ScriptFlush() (c scriptFlush) {
	c.cs = append(b.get(), "SCRIPT", "FLUSH")
	return
}

type scriptFlushAsyncAsync struct {
	cs []string
}

func (c scriptFlushAsyncAsync) Build() []string {
	return c.cs
}

type scriptFlushAsyncSync struct {
	cs []string
}

func (c scriptFlushAsyncSync) Build() []string {
	return c.cs
}

type scriptKill struct {
	cs []string
}

func (c scriptKill) Build() []string {
	return c.cs
}

func (b *Builder) ScriptKill() (c scriptKill) {
	c.cs = append(b.get(), "SCRIPT", "KILL")
	return
}

type scriptLoad struct {
	cs []string
}

func (c scriptLoad) Script(script string) scriptLoadScript {
	return scriptLoadScript{cs: append(c.cs, script)}
}

func (b *Builder) ScriptLoad() (c scriptLoad) {
	c.cs = append(b.get(), "SCRIPT", "LOAD")
	return
}

type scriptLoadScript struct {
	cs []string
}

func (c scriptLoadScript) Build() []string {
	return c.cs
}

type sdiff struct {
	cs []string
}

func (c sdiff) Key(key ...string) sdiffKey {
	return sdiffKey{cs: append(c.cs, key...)}
}

func (b *Builder) Sdiff() (c sdiff) {
	c.cs = append(b.get(), "SDIFF")
	return
}

type sdiffKey struct {
	cs []string
}

func (c sdiffKey) Key(key ...string) sdiffKey {
	return sdiffKey{cs: append(c.cs, key...)}
}

func (c sdiffKey) Build() []string {
	return c.cs
}

type sdiffstore struct {
	cs []string
}

func (c sdiffstore) Destination(destination string) sdiffstoreDestination {
	return sdiffstoreDestination{cs: append(c.cs, destination)}
}

func (b *Builder) Sdiffstore() (c sdiffstore) {
	c.cs = append(b.get(), "SDIFFSTORE")
	return
}

type sdiffstoreDestination struct {
	cs []string
}

func (c sdiffstoreDestination) Key(key ...string) sdiffstoreKey {
	return sdiffstoreKey{cs: append(c.cs, key...)}
}

type sdiffstoreKey struct {
	cs []string
}

func (c sdiffstoreKey) Key(key ...string) sdiffstoreKey {
	return sdiffstoreKey{cs: append(c.cs, key...)}
}

func (c sdiffstoreKey) Build() []string {
	return c.cs
}

type rSelect struct {
	cs []string
}

func (c rSelect) Index(index int64) selectIndex {
	return selectIndex{cs: append(c.cs, strconv.FormatInt(index, 10))}
}

func (b *Builder) Select() (c rSelect) {
	c.cs = append(b.get(), "SELECT")
	return
}

type selectIndex struct {
	cs []string
}

func (c selectIndex) Build() []string {
	return c.cs
}

type set struct {
	cs []string
}

func (c set) Key(key string) setKey {
	return setKey{cs: append(c.cs, key)}
}

func (b *Builder) Set() (c set) {
	c.cs = append(b.get(), "SET")
	return
}

type setConditionNx struct {
	cs []string
}

func (c setConditionNx) Get() setGetGet {
	return setGetGet{cs: append(c.cs, "GET")}
}

func (c setConditionNx) Build() []string {
	return c.cs
}

type setConditionXx struct {
	cs []string
}

func (c setConditionXx) Get() setGetGet {
	return setGetGet{cs: append(c.cs, "GET")}
}

func (c setConditionXx) Build() []string {
	return c.cs
}

type setExpirationEx struct {
	cs []string
}

func (c setExpirationEx) Nx() setConditionNx {
	return setConditionNx{cs: append(c.cs, "NX")}
}

func (c setExpirationEx) Xx() setConditionXx {
	return setConditionXx{cs: append(c.cs, "XX")}
}

func (c setExpirationEx) Get() setGetGet {
	return setGetGet{cs: append(c.cs, "GET")}
}

func (c setExpirationEx) Build() []string {
	return c.cs
}

type setExpirationExat struct {
	cs []string
}

func (c setExpirationExat) Nx() setConditionNx {
	return setConditionNx{cs: append(c.cs, "NX")}
}

func (c setExpirationExat) Xx() setConditionXx {
	return setConditionXx{cs: append(c.cs, "XX")}
}

func (c setExpirationExat) Get() setGetGet {
	return setGetGet{cs: append(c.cs, "GET")}
}

func (c setExpirationExat) Build() []string {
	return c.cs
}

type setExpirationKeepttl struct {
	cs []string
}

func (c setExpirationKeepttl) Nx() setConditionNx {
	return setConditionNx{cs: append(c.cs, "NX")}
}

func (c setExpirationKeepttl) Xx() setConditionXx {
	return setConditionXx{cs: append(c.cs, "XX")}
}

func (c setExpirationKeepttl) Get() setGetGet {
	return setGetGet{cs: append(c.cs, "GET")}
}

func (c setExpirationKeepttl) Build() []string {
	return c.cs
}

type setExpirationPx struct {
	cs []string
}

func (c setExpirationPx) Nx() setConditionNx {
	return setConditionNx{cs: append(c.cs, "NX")}
}

func (c setExpirationPx) Xx() setConditionXx {
	return setConditionXx{cs: append(c.cs, "XX")}
}

func (c setExpirationPx) Get() setGetGet {
	return setGetGet{cs: append(c.cs, "GET")}
}

func (c setExpirationPx) Build() []string {
	return c.cs
}

type setExpirationPxat struct {
	cs []string
}

func (c setExpirationPxat) Nx() setConditionNx {
	return setConditionNx{cs: append(c.cs, "NX")}
}

func (c setExpirationPxat) Xx() setConditionXx {
	return setConditionXx{cs: append(c.cs, "XX")}
}

func (c setExpirationPxat) Get() setGetGet {
	return setGetGet{cs: append(c.cs, "GET")}
}

func (c setExpirationPxat) Build() []string {
	return c.cs
}

type setGetGet struct {
	cs []string
}

func (c setGetGet) Build() []string {
	return c.cs
}

type setKey struct {
	cs []string
}

func (c setKey) Value(value string) setValue {
	return setValue{cs: append(c.cs, value)}
}

type setValue struct {
	cs []string
}

func (c setValue) Ex(seconds int64) setExpirationEx {
	return setExpirationEx{cs: append(c.cs, "EX", strconv.FormatInt(seconds, 10))}
}

func (c setValue) Px(milliseconds int64) setExpirationPx {
	return setExpirationPx{cs: append(c.cs, "PX", strconv.FormatInt(milliseconds, 10))}
}

func (c setValue) Exat(timestamp int64) setExpirationExat {
	return setExpirationExat{cs: append(c.cs, "EXAT", strconv.FormatInt(timestamp, 10))}
}

func (c setValue) Pxat(millisecondstimestamp int64) setExpirationPxat {
	return setExpirationPxat{cs: append(c.cs, "PXAT", strconv.FormatInt(millisecondstimestamp, 10))}
}

func (c setValue) Keepttl() setExpirationKeepttl {
	return setExpirationKeepttl{cs: append(c.cs, "KEEPTTL")}
}

func (c setValue) Nx() setConditionNx {
	return setConditionNx{cs: append(c.cs, "NX")}
}

func (c setValue) Xx() setConditionXx {
	return setConditionXx{cs: append(c.cs, "XX")}
}

func (c setValue) Get() setGetGet {
	return setGetGet{cs: append(c.cs, "GET")}
}

func (c setValue) Build() []string {
	return c.cs
}

type setbit struct {
	cs []string
}

func (c setbit) Key(key string) setbitKey {
	return setbitKey{cs: append(c.cs, key)}
}

func (b *Builder) Setbit() (c setbit) {
	c.cs = append(b.get(), "SETBIT")
	return
}

type setbitKey struct {
	cs []string
}

func (c setbitKey) Offset(offset int64) setbitOffset {
	return setbitOffset{cs: append(c.cs, strconv.FormatInt(offset, 10))}
}

type setbitOffset struct {
	cs []string
}

func (c setbitOffset) Value(value int64) setbitValue {
	return setbitValue{cs: append(c.cs, strconv.FormatInt(value, 10))}
}

type setbitValue struct {
	cs []string
}

func (c setbitValue) Build() []string {
	return c.cs
}

type setex struct {
	cs []string
}

func (c setex) Key(key string) setexKey {
	return setexKey{cs: append(c.cs, key)}
}

func (b *Builder) Setex() (c setex) {
	c.cs = append(b.get(), "SETEX")
	return
}

type setexKey struct {
	cs []string
}

func (c setexKey) Seconds(seconds int64) setexSeconds {
	return setexSeconds{cs: append(c.cs, strconv.FormatInt(seconds, 10))}
}

type setexSeconds struct {
	cs []string
}

func (c setexSeconds) Value(value string) setexValue {
	return setexValue{cs: append(c.cs, value)}
}

type setexValue struct {
	cs []string
}

func (c setexValue) Build() []string {
	return c.cs
}

type setnx struct {
	cs []string
}

func (c setnx) Key(key string) setnxKey {
	return setnxKey{cs: append(c.cs, key)}
}

func (b *Builder) Setnx() (c setnx) {
	c.cs = append(b.get(), "SETNX")
	return
}

type setnxKey struct {
	cs []string
}

func (c setnxKey) Value(value string) setnxValue {
	return setnxValue{cs: append(c.cs, value)}
}

type setnxValue struct {
	cs []string
}

func (c setnxValue) Build() []string {
	return c.cs
}

type setrange struct {
	cs []string
}

func (c setrange) Key(key string) setrangeKey {
	return setrangeKey{cs: append(c.cs, key)}
}

func (b *Builder) Setrange() (c setrange) {
	c.cs = append(b.get(), "SETRANGE")
	return
}

type setrangeKey struct {
	cs []string
}

func (c setrangeKey) Offset(offset int64) setrangeOffset {
	return setrangeOffset{cs: append(c.cs, strconv.FormatInt(offset, 10))}
}

type setrangeOffset struct {
	cs []string
}

func (c setrangeOffset) Value(value string) setrangeValue {
	return setrangeValue{cs: append(c.cs, value)}
}

type setrangeValue struct {
	cs []string
}

func (c setrangeValue) Build() []string {
	return c.cs
}

type shutdown struct {
	cs []string
}

func (c shutdown) Nosave() shutdownSaveModeNosave {
	return shutdownSaveModeNosave{cs: append(c.cs, "NOSAVE")}
}

func (c shutdown) Save() shutdownSaveModeSave {
	return shutdownSaveModeSave{cs: append(c.cs, "SAVE")}
}

func (c shutdown) Build() []string {
	return c.cs
}

func (b *Builder) Shutdown() (c shutdown) {
	c.cs = append(b.get(), "SHUTDOWN")
	return
}

type shutdownSaveModeNosave struct {
	cs []string
}

func (c shutdownSaveModeNosave) Build() []string {
	return c.cs
}

type shutdownSaveModeSave struct {
	cs []string
}

func (c shutdownSaveModeSave) Build() []string {
	return c.cs
}

type sinter struct {
	cs []string
}

func (c sinter) Key(key ...string) sinterKey {
	return sinterKey{cs: append(c.cs, key...)}
}

func (b *Builder) Sinter() (c sinter) {
	c.cs = append(b.get(), "SINTER")
	return
}

type sinterKey struct {
	cs []string
}

func (c sinterKey) Key(key ...string) sinterKey {
	return sinterKey{cs: append(c.cs, key...)}
}

func (c sinterKey) Build() []string {
	return c.cs
}

type sintercard struct {
	cs []string
}

func (c sintercard) Key(key ...string) sintercardKey {
	return sintercardKey{cs: append(c.cs, key...)}
}

func (b *Builder) Sintercard() (c sintercard) {
	c.cs = append(b.get(), "SINTERCARD")
	return
}

type sintercardKey struct {
	cs []string
}

func (c sintercardKey) Key(key ...string) sintercardKey {
	return sintercardKey{cs: append(c.cs, key...)}
}

func (c sintercardKey) Build() []string {
	return c.cs
}

type sinterstore struct {
	cs []string
}

func (c sinterstore) Destination(destination string) sinterstoreDestination {
	return sinterstoreDestination{cs: append(c.cs, destination)}
}

func (b *Builder) Sinterstore() (c sinterstore) {
	c.cs = append(b.get(), "SINTERSTORE")
	return
}

type sinterstoreDestination struct {
	cs []string
}

func (c sinterstoreDestination) Key(key ...string) sinterstoreKey {
	return sinterstoreKey{cs: append(c.cs, key...)}
}

type sinterstoreKey struct {
	cs []string
}

func (c sinterstoreKey) Key(key ...string) sinterstoreKey {
	return sinterstoreKey{cs: append(c.cs, key...)}
}

func (c sinterstoreKey) Build() []string {
	return c.cs
}

type sismember struct {
	cs []string
}

func (c sismember) Key(key string) sismemberKey {
	return sismemberKey{cs: append(c.cs, key)}
}

func (b *Builder) Sismember() (c sismember) {
	c.cs = append(b.get(), "SISMEMBER")
	return
}

type sismemberKey struct {
	cs []string
}

func (c sismemberKey) Member(member string) sismemberMember {
	return sismemberMember{cs: append(c.cs, member)}
}

type sismemberMember struct {
	cs []string
}

func (c sismemberMember) Build() []string {
	return c.cs
}

type slaveof struct {
	cs []string
}

func (c slaveof) Host(host string) slaveofHost {
	return slaveofHost{cs: append(c.cs, host)}
}

func (b *Builder) Slaveof() (c slaveof) {
	c.cs = append(b.get(), "SLAVEOF")
	return
}

type slaveofHost struct {
	cs []string
}

func (c slaveofHost) Port(port string) slaveofPort {
	return slaveofPort{cs: append(c.cs, port)}
}

type slaveofPort struct {
	cs []string
}

func (c slaveofPort) Build() []string {
	return c.cs
}

type slowlog struct {
	cs []string
}

func (c slowlog) Subcommand(subcommand string) slowlogSubcommand {
	return slowlogSubcommand{cs: append(c.cs, subcommand)}
}

func (b *Builder) Slowlog() (c slowlog) {
	c.cs = append(b.get(), "SLOWLOG")
	return
}

type slowlogArgument struct {
	cs []string
}

func (c slowlogArgument) Build() []string {
	return c.cs
}

type slowlogSubcommand struct {
	cs []string
}

func (c slowlogSubcommand) Argument(argument string) slowlogArgument {
	return slowlogArgument{cs: append(c.cs, argument)}
}

func (c slowlogSubcommand) Build() []string {
	return c.cs
}

type smembers struct {
	cs []string
}

func (c smembers) Key(key string) smembersKey {
	return smembersKey{cs: append(c.cs, key)}
}

func (b *Builder) Smembers() (c smembers) {
	c.cs = append(b.get(), "SMEMBERS")
	return
}

type smembersKey struct {
	cs []string
}

func (c smembersKey) Build() []string {
	return c.cs
}

type smismember struct {
	cs []string
}

func (c smismember) Key(key string) smismemberKey {
	return smismemberKey{cs: append(c.cs, key)}
}

func (b *Builder) Smismember() (c smismember) {
	c.cs = append(b.get(), "SMISMEMBER")
	return
}

type smismemberKey struct {
	cs []string
}

func (c smismemberKey) Member(member ...string) smismemberMember {
	return smismemberMember{cs: append(c.cs, member...)}
}

type smismemberMember struct {
	cs []string
}

func (c smismemberMember) Member(member ...string) smismemberMember {
	return smismemberMember{cs: append(c.cs, member...)}
}

func (c smismemberMember) Build() []string {
	return c.cs
}

type smove struct {
	cs []string
}

func (c smove) Source(source string) smoveSource {
	return smoveSource{cs: append(c.cs, source)}
}

func (b *Builder) Smove() (c smove) {
	c.cs = append(b.get(), "SMOVE")
	return
}

type smoveDestination struct {
	cs []string
}

func (c smoveDestination) Member(member string) smoveMember {
	return smoveMember{cs: append(c.cs, member)}
}

type smoveMember struct {
	cs []string
}

func (c smoveMember) Build() []string {
	return c.cs
}

type smoveSource struct {
	cs []string
}

func (c smoveSource) Destination(destination string) smoveDestination {
	return smoveDestination{cs: append(c.cs, destination)}
}

type sort struct {
	cs []string
}

func (c sort) Key(key string) sortKey {
	return sortKey{cs: append(c.cs, key)}
}

func (b *Builder) Sort() (c sort) {
	c.cs = append(b.get(), "SORT")
	return
}

type sortBy struct {
	cs []string
}

func (c sortBy) Limit(offset int64, count int64) sortLimit {
	return sortLimit{cs: append(c.cs, "LIMIT", strconv.FormatInt(offset, 10), strconv.FormatInt(count, 10))}
}

func (c sortBy) Get(pattern ...string) sortGet {
	c.cs = append(c.cs, "GET")
	return sortGet{cs: append(c.cs, pattern...)}
}

func (c sortBy) Asc() sortOrderAsc {
	return sortOrderAsc{cs: append(c.cs, "ASC")}
}

func (c sortBy) Desc() sortOrderDesc {
	return sortOrderDesc{cs: append(c.cs, "DESC")}
}

func (c sortBy) Alpha() sortSortingAlpha {
	return sortSortingAlpha{cs: append(c.cs, "ALPHA")}
}

func (c sortBy) Store(destination string) sortStore {
	return sortStore{cs: append(c.cs, "STORE", destination)}
}

func (c sortBy) Build() []string {
	return c.cs
}

type sortGet struct {
	cs []string
}

func (c sortGet) Asc() sortOrderAsc {
	return sortOrderAsc{cs: append(c.cs, "ASC")}
}

func (c sortGet) Desc() sortOrderDesc {
	return sortOrderDesc{cs: append(c.cs, "DESC")}
}

func (c sortGet) Alpha() sortSortingAlpha {
	return sortSortingAlpha{cs: append(c.cs, "ALPHA")}
}

func (c sortGet) Store(destination string) sortStore {
	return sortStore{cs: append(c.cs, "STORE", destination)}
}

func (c sortGet) Get(get ...string) sortGet {
	return sortGet{cs: append(c.cs, get...)}
}

func (c sortGet) Build() []string {
	return c.cs
}

type sortKey struct {
	cs []string
}

func (c sortKey) By(pattern string) sortBy {
	return sortBy{cs: append(c.cs, "BY", pattern)}
}

func (c sortKey) Limit(offset int64, count int64) sortLimit {
	return sortLimit{cs: append(c.cs, "LIMIT", strconv.FormatInt(offset, 10), strconv.FormatInt(count, 10))}
}

func (c sortKey) Get(pattern ...string) sortGet {
	c.cs = append(c.cs, "GET")
	return sortGet{cs: append(c.cs, pattern...)}
}

func (c sortKey) Asc() sortOrderAsc {
	return sortOrderAsc{cs: append(c.cs, "ASC")}
}

func (c sortKey) Desc() sortOrderDesc {
	return sortOrderDesc{cs: append(c.cs, "DESC")}
}

func (c sortKey) Alpha() sortSortingAlpha {
	return sortSortingAlpha{cs: append(c.cs, "ALPHA")}
}

func (c sortKey) Store(destination string) sortStore {
	return sortStore{cs: append(c.cs, "STORE", destination)}
}

func (c sortKey) Build() []string {
	return c.cs
}

type sortLimit struct {
	cs []string
}

func (c sortLimit) Get(pattern ...string) sortGet {
	c.cs = append(c.cs, "GET")
	return sortGet{cs: append(c.cs, pattern...)}
}

func (c sortLimit) Asc() sortOrderAsc {
	return sortOrderAsc{cs: append(c.cs, "ASC")}
}

func (c sortLimit) Desc() sortOrderDesc {
	return sortOrderDesc{cs: append(c.cs, "DESC")}
}

func (c sortLimit) Alpha() sortSortingAlpha {
	return sortSortingAlpha{cs: append(c.cs, "ALPHA")}
}

func (c sortLimit) Store(destination string) sortStore {
	return sortStore{cs: append(c.cs, "STORE", destination)}
}

func (c sortLimit) Build() []string {
	return c.cs
}

type sortOrderAsc struct {
	cs []string
}

func (c sortOrderAsc) Alpha() sortSortingAlpha {
	return sortSortingAlpha{cs: append(c.cs, "ALPHA")}
}

func (c sortOrderAsc) Store(destination string) sortStore {
	return sortStore{cs: append(c.cs, "STORE", destination)}
}

func (c sortOrderAsc) Build() []string {
	return c.cs
}

type sortOrderDesc struct {
	cs []string
}

func (c sortOrderDesc) Alpha() sortSortingAlpha {
	return sortSortingAlpha{cs: append(c.cs, "ALPHA")}
}

func (c sortOrderDesc) Store(destination string) sortStore {
	return sortStore{cs: append(c.cs, "STORE", destination)}
}

func (c sortOrderDesc) Build() []string {
	return c.cs
}

type sortRo struct {
	cs []string
}

func (c sortRo) Key(key string) sortRoKey {
	return sortRoKey{cs: append(c.cs, key)}
}

func (b *Builder) SortRo() (c sortRo) {
	c.cs = append(b.get(), "SORT_RO")
	return
}

type sortRoBy struct {
	cs []string
}

func (c sortRoBy) Limit(offset int64, count int64) sortRoLimit {
	return sortRoLimit{cs: append(c.cs, "LIMIT", strconv.FormatInt(offset, 10), strconv.FormatInt(count, 10))}
}

func (c sortRoBy) Get(pattern ...string) sortRoGet {
	c.cs = append(c.cs, "GET")
	return sortRoGet{cs: append(c.cs, pattern...)}
}

func (c sortRoBy) Asc() sortRoOrderAsc {
	return sortRoOrderAsc{cs: append(c.cs, "ASC")}
}

func (c sortRoBy) Desc() sortRoOrderDesc {
	return sortRoOrderDesc{cs: append(c.cs, "DESC")}
}

func (c sortRoBy) Alpha() sortRoSortingAlpha {
	return sortRoSortingAlpha{cs: append(c.cs, "ALPHA")}
}

func (c sortRoBy) Build() []string {
	return c.cs
}

type sortRoGet struct {
	cs []string
}

func (c sortRoGet) Asc() sortRoOrderAsc {
	return sortRoOrderAsc{cs: append(c.cs, "ASC")}
}

func (c sortRoGet) Desc() sortRoOrderDesc {
	return sortRoOrderDesc{cs: append(c.cs, "DESC")}
}

func (c sortRoGet) Alpha() sortRoSortingAlpha {
	return sortRoSortingAlpha{cs: append(c.cs, "ALPHA")}
}

func (c sortRoGet) Get(get ...string) sortRoGet {
	return sortRoGet{cs: append(c.cs, get...)}
}

func (c sortRoGet) Build() []string {
	return c.cs
}

type sortRoKey struct {
	cs []string
}

func (c sortRoKey) By(pattern string) sortRoBy {
	return sortRoBy{cs: append(c.cs, "BY", pattern)}
}

func (c sortRoKey) Limit(offset int64, count int64) sortRoLimit {
	return sortRoLimit{cs: append(c.cs, "LIMIT", strconv.FormatInt(offset, 10), strconv.FormatInt(count, 10))}
}

func (c sortRoKey) Get(pattern ...string) sortRoGet {
	c.cs = append(c.cs, "GET")
	return sortRoGet{cs: append(c.cs, pattern...)}
}

func (c sortRoKey) Asc() sortRoOrderAsc {
	return sortRoOrderAsc{cs: append(c.cs, "ASC")}
}

func (c sortRoKey) Desc() sortRoOrderDesc {
	return sortRoOrderDesc{cs: append(c.cs, "DESC")}
}

func (c sortRoKey) Alpha() sortRoSortingAlpha {
	return sortRoSortingAlpha{cs: append(c.cs, "ALPHA")}
}

func (c sortRoKey) Build() []string {
	return c.cs
}

type sortRoLimit struct {
	cs []string
}

func (c sortRoLimit) Get(pattern ...string) sortRoGet {
	c.cs = append(c.cs, "GET")
	return sortRoGet{cs: append(c.cs, pattern...)}
}

func (c sortRoLimit) Asc() sortRoOrderAsc {
	return sortRoOrderAsc{cs: append(c.cs, "ASC")}
}

func (c sortRoLimit) Desc() sortRoOrderDesc {
	return sortRoOrderDesc{cs: append(c.cs, "DESC")}
}

func (c sortRoLimit) Alpha() sortRoSortingAlpha {
	return sortRoSortingAlpha{cs: append(c.cs, "ALPHA")}
}

func (c sortRoLimit) Build() []string {
	return c.cs
}

type sortRoOrderAsc struct {
	cs []string
}

func (c sortRoOrderAsc) Alpha() sortRoSortingAlpha {
	return sortRoSortingAlpha{cs: append(c.cs, "ALPHA")}
}

func (c sortRoOrderAsc) Build() []string {
	return c.cs
}

type sortRoOrderDesc struct {
	cs []string
}

func (c sortRoOrderDesc) Alpha() sortRoSortingAlpha {
	return sortRoSortingAlpha{cs: append(c.cs, "ALPHA")}
}

func (c sortRoOrderDesc) Build() []string {
	return c.cs
}

type sortRoSortingAlpha struct {
	cs []string
}

func (c sortRoSortingAlpha) Build() []string {
	return c.cs
}

type sortSortingAlpha struct {
	cs []string
}

func (c sortSortingAlpha) Store(destination string) sortStore {
	return sortStore{cs: append(c.cs, "STORE", destination)}
}

func (c sortSortingAlpha) Build() []string {
	return c.cs
}

type sortStore struct {
	cs []string
}

func (c sortStore) Build() []string {
	return c.cs
}

type spop struct {
	cs []string
}

func (c spop) Key(key string) spopKey {
	return spopKey{cs: append(c.cs, key)}
}

func (b *Builder) Spop() (c spop) {
	c.cs = append(b.get(), "SPOP")
	return
}

type spopCount struct {
	cs []string
}

func (c spopCount) Build() []string {
	return c.cs
}

type spopKey struct {
	cs []string
}

func (c spopKey) Count(count int64) spopCount {
	return spopCount{cs: append(c.cs, strconv.FormatInt(count, 10))}
}

func (c spopKey) Build() []string {
	return c.cs
}

type srandmember struct {
	cs []string
}

func (c srandmember) Key(key string) srandmemberKey {
	return srandmemberKey{cs: append(c.cs, key)}
}

func (b *Builder) Srandmember() (c srandmember) {
	c.cs = append(b.get(), "SRANDMEMBER")
	return
}

type srandmemberCount struct {
	cs []string
}

func (c srandmemberCount) Build() []string {
	return c.cs
}

type srandmemberKey struct {
	cs []string
}

func (c srandmemberKey) Count(count int64) srandmemberCount {
	return srandmemberCount{cs: append(c.cs, strconv.FormatInt(count, 10))}
}

func (c srandmemberKey) Build() []string {
	return c.cs
}

type srem struct {
	cs []string
}

func (c srem) Key(key string) sremKey {
	return sremKey{cs: append(c.cs, key)}
}

func (b *Builder) Srem() (c srem) {
	c.cs = append(b.get(), "SREM")
	return
}

type sremKey struct {
	cs []string
}

func (c sremKey) Member(member ...string) sremMember {
	return sremMember{cs: append(c.cs, member...)}
}

type sremMember struct {
	cs []string
}

func (c sremMember) Member(member ...string) sremMember {
	return sremMember{cs: append(c.cs, member...)}
}

func (c sremMember) Build() []string {
	return c.cs
}

type sscan struct {
	cs []string
}

func (c sscan) Key(key string) sscanKey {
	return sscanKey{cs: append(c.cs, key)}
}

func (b *Builder) Sscan() (c sscan) {
	c.cs = append(b.get(), "SSCAN")
	return
}

type sscanCount struct {
	cs []string
}

func (c sscanCount) Build() []string {
	return c.cs
}

type sscanCursor struct {
	cs []string
}

func (c sscanCursor) Match(pattern string) sscanMatch {
	return sscanMatch{cs: append(c.cs, "MATCH", pattern)}
}

func (c sscanCursor) Count(count int64) sscanCount {
	return sscanCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c sscanCursor) Build() []string {
	return c.cs
}

type sscanKey struct {
	cs []string
}

func (c sscanKey) Cursor(cursor int64) sscanCursor {
	return sscanCursor{cs: append(c.cs, strconv.FormatInt(cursor, 10))}
}

type sscanMatch struct {
	cs []string
}

func (c sscanMatch) Count(count int64) sscanCount {
	return sscanCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c sscanMatch) Build() []string {
	return c.cs
}

type stralgo struct {
	cs []string
}

func (c stralgo) Lcs() stralgoAlgorithmLcs {
	return stralgoAlgorithmLcs{cs: append(c.cs, "LCS")}
}

func (b *Builder) Stralgo() (c stralgo) {
	c.cs = append(b.get(), "STRALGO")
	return
}

type stralgoAlgoSpecificArgument struct {
	cs []string
}

func (c stralgoAlgoSpecificArgument) AlgoSpecificArgument(algoSpecificArgument ...string) stralgoAlgoSpecificArgument {
	return stralgoAlgoSpecificArgument{cs: append(c.cs, algoSpecificArgument...)}
}

func (c stralgoAlgoSpecificArgument) Build() []string {
	return c.cs
}

type stralgoAlgorithmLcs struct {
	cs []string
}

func (c stralgoAlgorithmLcs) AlgoSpecificArgument(algoSpecificArgument ...string) stralgoAlgoSpecificArgument {
	return stralgoAlgoSpecificArgument{cs: append(c.cs, algoSpecificArgument...)}
}

type strlen struct {
	cs []string
}

func (c strlen) Key(key string) strlenKey {
	return strlenKey{cs: append(c.cs, key)}
}

func (b *Builder) Strlen() (c strlen) {
	c.cs = append(b.get(), "STRLEN")
	return
}

type strlenKey struct {
	cs []string
}

func (c strlenKey) Build() []string {
	return c.cs
}

type subscribe struct {
	cs []string
}

func (c subscribe) Channel(channel ...string) subscribeChannel {
	return subscribeChannel{cs: append(c.cs, channel...)}
}

func (b *Builder) Subscribe() (c subscribe) {
	c.cs = append(b.get(), "SUBSCRIBE")
	return
}

type subscribeChannel struct {
	cs []string
}

func (c subscribeChannel) Channel(channel ...string) subscribeChannel {
	return subscribeChannel{cs: append(c.cs, channel...)}
}

func (c subscribeChannel) Build() []string {
	return c.cs
}

type sunion struct {
	cs []string
}

func (c sunion) Key(key ...string) sunionKey {
	return sunionKey{cs: append(c.cs, key...)}
}

func (b *Builder) Sunion() (c sunion) {
	c.cs = append(b.get(), "SUNION")
	return
}

type sunionKey struct {
	cs []string
}

func (c sunionKey) Key(key ...string) sunionKey {
	return sunionKey{cs: append(c.cs, key...)}
}

func (c sunionKey) Build() []string {
	return c.cs
}

type sunionstore struct {
	cs []string
}

func (c sunionstore) Destination(destination string) sunionstoreDestination {
	return sunionstoreDestination{cs: append(c.cs, destination)}
}

func (b *Builder) Sunionstore() (c sunionstore) {
	c.cs = append(b.get(), "SUNIONSTORE")
	return
}

type sunionstoreDestination struct {
	cs []string
}

func (c sunionstoreDestination) Key(key ...string) sunionstoreKey {
	return sunionstoreKey{cs: append(c.cs, key...)}
}

type sunionstoreKey struct {
	cs []string
}

func (c sunionstoreKey) Key(key ...string) sunionstoreKey {
	return sunionstoreKey{cs: append(c.cs, key...)}
}

func (c sunionstoreKey) Build() []string {
	return c.cs
}

type swapdb struct {
	cs []string
}

func (c swapdb) Index1(index1 int64) swapdbIndex1 {
	return swapdbIndex1{cs: append(c.cs, strconv.FormatInt(index1, 10))}
}

func (b *Builder) Swapdb() (c swapdb) {
	c.cs = append(b.get(), "SWAPDB")
	return
}

type swapdbIndex1 struct {
	cs []string
}

func (c swapdbIndex1) Index2(index2 int64) swapdbIndex2 {
	return swapdbIndex2{cs: append(c.cs, strconv.FormatInt(index2, 10))}
}

type swapdbIndex2 struct {
	cs []string
}

func (c swapdbIndex2) Build() []string {
	return c.cs
}

type rSync struct {
	cs []string
}

func (c rSync) Build() []string {
	return c.cs
}

func (b *Builder) Sync() (c rSync) {
	c.cs = append(b.get(), "SYNC")
	return
}

type time struct {
	cs []string
}

func (c time) Build() []string {
	return c.cs
}

func (b *Builder) Time() (c time) {
	c.cs = append(b.get(), "TIME")
	return
}

type touch struct {
	cs []string
}

func (c touch) Key(key ...string) touchKey {
	return touchKey{cs: append(c.cs, key...)}
}

func (b *Builder) Touch() (c touch) {
	c.cs = append(b.get(), "TOUCH")
	return
}

type touchKey struct {
	cs []string
}

func (c touchKey) Key(key ...string) touchKey {
	return touchKey{cs: append(c.cs, key...)}
}

func (c touchKey) Build() []string {
	return c.cs
}

type ttl struct {
	cs []string
}

func (c ttl) Key(key string) ttlKey {
	return ttlKey{cs: append(c.cs, key)}
}

func (b *Builder) Ttl() (c ttl) {
	c.cs = append(b.get(), "TTL")
	return
}

type ttlKey struct {
	cs []string
}

func (c ttlKey) Build() []string {
	return c.cs
}

type rType struct {
	cs []string
}

func (c rType) Key(key string) typeKey {
	return typeKey{cs: append(c.cs, key)}
}

func (b *Builder) Type() (c rType) {
	c.cs = append(b.get(), "TYPE")
	return
}

type typeKey struct {
	cs []string
}

func (c typeKey) Build() []string {
	return c.cs
}

type unlink struct {
	cs []string
}

func (c unlink) Key(key ...string) unlinkKey {
	return unlinkKey{cs: append(c.cs, key...)}
}

func (b *Builder) Unlink() (c unlink) {
	c.cs = append(b.get(), "UNLINK")
	return
}

type unlinkKey struct {
	cs []string
}

func (c unlinkKey) Key(key ...string) unlinkKey {
	return unlinkKey{cs: append(c.cs, key...)}
}

func (c unlinkKey) Build() []string {
	return c.cs
}

type unsubscribe struct {
	cs []string
}

func (c unsubscribe) Channel(channel ...string) unsubscribeChannel {
	return unsubscribeChannel{cs: append(c.cs, channel...)}
}

func (c unsubscribe) Build() []string {
	return c.cs
}

func (b *Builder) Unsubscribe() (c unsubscribe) {
	c.cs = append(b.get(), "UNSUBSCRIBE")
	return
}

type unsubscribeChannel struct {
	cs []string
}

func (c unsubscribeChannel) Channel(channel ...string) unsubscribeChannel {
	return unsubscribeChannel{cs: append(c.cs, channel...)}
}

func (c unsubscribeChannel) Build() []string {
	return c.cs
}

type unwatch struct {
	cs []string
}

func (c unwatch) Build() []string {
	return c.cs
}

func (b *Builder) Unwatch() (c unwatch) {
	c.cs = append(b.get(), "UNWATCH")
	return
}

type wait struct {
	cs []string
}

func (c wait) Numreplicas(numreplicas int64) waitNumreplicas {
	return waitNumreplicas{cs: append(c.cs, strconv.FormatInt(numreplicas, 10))}
}

func (b *Builder) Wait() (c wait) {
	c.cs = append(b.get(), "WAIT")
	return
}

type waitNumreplicas struct {
	cs []string
}

func (c waitNumreplicas) Timeout(timeout int64) waitTimeout {
	return waitTimeout{cs: append(c.cs, strconv.FormatInt(timeout, 10))}
}

type waitTimeout struct {
	cs []string
}

func (c waitTimeout) Build() []string {
	return c.cs
}

type watch struct {
	cs []string
}

func (c watch) Key(key ...string) watchKey {
	return watchKey{cs: append(c.cs, key...)}
}

func (b *Builder) Watch() (c watch) {
	c.cs = append(b.get(), "WATCH")
	return
}

type watchKey struct {
	cs []string
}

func (c watchKey) Key(key ...string) watchKey {
	return watchKey{cs: append(c.cs, key...)}
}

func (c watchKey) Build() []string {
	return c.cs
}

type xack struct {
	cs []string
}

func (c xack) Key(key string) xackKey {
	return xackKey{cs: append(c.cs, key)}
}

func (b *Builder) Xack() (c xack) {
	c.cs = append(b.get(), "XACK")
	return
}

type xackGroup struct {
	cs []string
}

func (c xackGroup) Id(id ...string) xackId {
	return xackId{cs: append(c.cs, id...)}
}

type xackId struct {
	cs []string
}

func (c xackId) Id(id ...string) xackId {
	return xackId{cs: append(c.cs, id...)}
}

func (c xackId) Build() []string {
	return c.cs
}

type xackKey struct {
	cs []string
}

func (c xackKey) Group(group string) xackGroup {
	return xackGroup{cs: append(c.cs, group)}
}

type xadd struct {
	cs []string
}

func (c xadd) Key(key string) xaddKey {
	return xaddKey{cs: append(c.cs, key)}
}

func (b *Builder) Xadd() (c xadd) {
	c.cs = append(b.get(), "XADD")
	return
}

type xaddFieldValue struct {
	cs []string
}

func (c xaddFieldValue) FieldValue(field string, value string) xaddFieldValue {
	return xaddFieldValue{cs: append(c.cs, field, value)}
}

func (c xaddFieldValue) Build() []string {
	return c.cs
}

type xaddId struct {
	cs []string
}

func (c xaddId) FieldValue() xaddFieldValue {
	return xaddFieldValue{cs: append(c.cs)}
}

type xaddKey struct {
	cs []string
}

func (c xaddKey) Nomkstream() xaddNomkstream {
	return xaddNomkstream{cs: append(c.cs, "NOMKSTREAM")}
}

func (c xaddKey) Maxlen() xaddTrimStrategyMaxlen {
	return xaddTrimStrategyMaxlen{cs: append(c.cs, "MAXLEN")}
}

func (c xaddKey) Minid() xaddTrimStrategyMinid {
	return xaddTrimStrategyMinid{cs: append(c.cs, "MINID")}
}

func (c xaddKey) Wildcard() xaddWildcard {
	return xaddWildcard{cs: append(c.cs, "*")}
}

func (c xaddKey) Id() xaddId {
	return xaddId{cs: append(c.cs, "ID")}
}

type xaddNomkstream struct {
	cs []string
}

func (c xaddNomkstream) Maxlen() xaddTrimStrategyMaxlen {
	return xaddTrimStrategyMaxlen{cs: append(c.cs, "MAXLEN")}
}

func (c xaddNomkstream) Minid() xaddTrimStrategyMinid {
	return xaddTrimStrategyMinid{cs: append(c.cs, "MINID")}
}

func (c xaddNomkstream) Wildcard() xaddWildcard {
	return xaddWildcard{cs: append(c.cs, "*")}
}

func (c xaddNomkstream) Id() xaddId {
	return xaddId{cs: append(c.cs, "ID")}
}

type xaddTrimLimit struct {
	cs []string
}

func (c xaddTrimLimit) Wildcard() xaddWildcard {
	return xaddWildcard{cs: append(c.cs, "*")}
}

func (c xaddTrimLimit) Id() xaddId {
	return xaddId{cs: append(c.cs, "ID")}
}

type xaddTrimOperatorAlmost struct {
	cs []string
}

func (c xaddTrimOperatorAlmost) Threshold(threshold string) xaddTrimThreshold {
	return xaddTrimThreshold{cs: append(c.cs, threshold)}
}

type xaddTrimOperatorExact struct {
	cs []string
}

func (c xaddTrimOperatorExact) Threshold(threshold string) xaddTrimThreshold {
	return xaddTrimThreshold{cs: append(c.cs, threshold)}
}

type xaddTrimStrategyMaxlen struct {
	cs []string
}

func (c xaddTrimStrategyMaxlen) Exact() xaddTrimOperatorExact {
	return xaddTrimOperatorExact{cs: append(c.cs, "=")}
}

func (c xaddTrimStrategyMaxlen) Almost() xaddTrimOperatorAlmost {
	return xaddTrimOperatorAlmost{cs: append(c.cs, "~")}
}

func (c xaddTrimStrategyMaxlen) Threshold(threshold string) xaddTrimThreshold {
	return xaddTrimThreshold{cs: append(c.cs, threshold)}
}

type xaddTrimStrategyMinid struct {
	cs []string
}

func (c xaddTrimStrategyMinid) Exact() xaddTrimOperatorExact {
	return xaddTrimOperatorExact{cs: append(c.cs, "=")}
}

func (c xaddTrimStrategyMinid) Almost() xaddTrimOperatorAlmost {
	return xaddTrimOperatorAlmost{cs: append(c.cs, "~")}
}

func (c xaddTrimStrategyMinid) Threshold(threshold string) xaddTrimThreshold {
	return xaddTrimThreshold{cs: append(c.cs, threshold)}
}

type xaddTrimThreshold struct {
	cs []string
}

func (c xaddTrimThreshold) Limit(count int64) xaddTrimLimit {
	return xaddTrimLimit{cs: append(c.cs, "LIMIT", strconv.FormatInt(count, 10))}
}

func (c xaddTrimThreshold) Wildcard() xaddWildcard {
	return xaddWildcard{cs: append(c.cs, "*")}
}

func (c xaddTrimThreshold) Id() xaddId {
	return xaddId{cs: append(c.cs, "ID")}
}

type xaddWildcard struct {
	cs []string
}

func (c xaddWildcard) FieldValue() xaddFieldValue {
	return xaddFieldValue{cs: append(c.cs)}
}

type xautoclaim struct {
	cs []string
}

func (c xautoclaim) Key(key string) xautoclaimKey {
	return xautoclaimKey{cs: append(c.cs, key)}
}

func (b *Builder) Xautoclaim() (c xautoclaim) {
	c.cs = append(b.get(), "XAUTOCLAIM")
	return
}

type xautoclaimConsumer struct {
	cs []string
}

func (c xautoclaimConsumer) MinIdleTime(minIdleTime string) xautoclaimMinIdleTime {
	return xautoclaimMinIdleTime{cs: append(c.cs, minIdleTime)}
}

type xautoclaimCount struct {
	cs []string
}

func (c xautoclaimCount) Justid() xautoclaimJustidJustid {
	return xautoclaimJustidJustid{cs: append(c.cs, "JUSTID")}
}

func (c xautoclaimCount) Build() []string {
	return c.cs
}

type xautoclaimGroup struct {
	cs []string
}

func (c xautoclaimGroup) Consumer(consumer string) xautoclaimConsumer {
	return xautoclaimConsumer{cs: append(c.cs, consumer)}
}

type xautoclaimJustidJustid struct {
	cs []string
}

func (c xautoclaimJustidJustid) Build() []string {
	return c.cs
}

type xautoclaimKey struct {
	cs []string
}

func (c xautoclaimKey) Group(group string) xautoclaimGroup {
	return xautoclaimGroup{cs: append(c.cs, group)}
}

type xautoclaimMinIdleTime struct {
	cs []string
}

func (c xautoclaimMinIdleTime) Start(start string) xautoclaimStart {
	return xautoclaimStart{cs: append(c.cs, start)}
}

type xautoclaimStart struct {
	cs []string
}

func (c xautoclaimStart) Count(count int64) xautoclaimCount {
	return xautoclaimCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c xautoclaimStart) Justid() xautoclaimJustidJustid {
	return xautoclaimJustidJustid{cs: append(c.cs, "JUSTID")}
}

func (c xautoclaimStart) Build() []string {
	return c.cs
}

type xclaim struct {
	cs []string
}

func (c xclaim) Key(key string) xclaimKey {
	return xclaimKey{cs: append(c.cs, key)}
}

func (b *Builder) Xclaim() (c xclaim) {
	c.cs = append(b.get(), "XCLAIM")
	return
}

type xclaimConsumer struct {
	cs []string
}

func (c xclaimConsumer) MinIdleTime(minIdleTime string) xclaimMinIdleTime {
	return xclaimMinIdleTime{cs: append(c.cs, minIdleTime)}
}

type xclaimForceForce struct {
	cs []string
}

func (c xclaimForceForce) Justid() xclaimJustidJustid {
	return xclaimJustidJustid{cs: append(c.cs, "JUSTID")}
}

func (c xclaimForceForce) Build() []string {
	return c.cs
}

type xclaimGroup struct {
	cs []string
}

func (c xclaimGroup) Consumer(consumer string) xclaimConsumer {
	return xclaimConsumer{cs: append(c.cs, consumer)}
}

type xclaimId struct {
	cs []string
}

func (c xclaimId) Idle(ms int64) xclaimIdle {
	return xclaimIdle{cs: append(c.cs, "IDLE", strconv.FormatInt(ms, 10))}
}

func (c xclaimId) Time(msUnixTime int64) xclaimTime {
	return xclaimTime{cs: append(c.cs, "TIME", strconv.FormatInt(msUnixTime, 10))}
}

func (c xclaimId) Retrycount(count int64) xclaimRetrycount {
	return xclaimRetrycount{cs: append(c.cs, "RETRYCOUNT", strconv.FormatInt(count, 10))}
}

func (c xclaimId) Force() xclaimForceForce {
	return xclaimForceForce{cs: append(c.cs, "FORCE")}
}

func (c xclaimId) Justid() xclaimJustidJustid {
	return xclaimJustidJustid{cs: append(c.cs, "JUSTID")}
}

func (c xclaimId) Id(id ...string) xclaimId {
	return xclaimId{cs: append(c.cs, id...)}
}

func (c xclaimId) Build() []string {
	return c.cs
}

type xclaimIdle struct {
	cs []string
}

func (c xclaimIdle) Time(msUnixTime int64) xclaimTime {
	return xclaimTime{cs: append(c.cs, "TIME", strconv.FormatInt(msUnixTime, 10))}
}

func (c xclaimIdle) Retrycount(count int64) xclaimRetrycount {
	return xclaimRetrycount{cs: append(c.cs, "RETRYCOUNT", strconv.FormatInt(count, 10))}
}

func (c xclaimIdle) Force() xclaimForceForce {
	return xclaimForceForce{cs: append(c.cs, "FORCE")}
}

func (c xclaimIdle) Justid() xclaimJustidJustid {
	return xclaimJustidJustid{cs: append(c.cs, "JUSTID")}
}

func (c xclaimIdle) Build() []string {
	return c.cs
}

type xclaimJustidJustid struct {
	cs []string
}

func (c xclaimJustidJustid) Build() []string {
	return c.cs
}

type xclaimKey struct {
	cs []string
}

func (c xclaimKey) Group(group string) xclaimGroup {
	return xclaimGroup{cs: append(c.cs, group)}
}

type xclaimMinIdleTime struct {
	cs []string
}

func (c xclaimMinIdleTime) Id(id ...string) xclaimId {
	return xclaimId{cs: append(c.cs, id...)}
}

type xclaimRetrycount struct {
	cs []string
}

func (c xclaimRetrycount) Force() xclaimForceForce {
	return xclaimForceForce{cs: append(c.cs, "FORCE")}
}

func (c xclaimRetrycount) Justid() xclaimJustidJustid {
	return xclaimJustidJustid{cs: append(c.cs, "JUSTID")}
}

func (c xclaimRetrycount) Build() []string {
	return c.cs
}

type xclaimTime struct {
	cs []string
}

func (c xclaimTime) Retrycount(count int64) xclaimRetrycount {
	return xclaimRetrycount{cs: append(c.cs, "RETRYCOUNT", strconv.FormatInt(count, 10))}
}

func (c xclaimTime) Force() xclaimForceForce {
	return xclaimForceForce{cs: append(c.cs, "FORCE")}
}

func (c xclaimTime) Justid() xclaimJustidJustid {
	return xclaimJustidJustid{cs: append(c.cs, "JUSTID")}
}

func (c xclaimTime) Build() []string {
	return c.cs
}

type xdel struct {
	cs []string
}

func (c xdel) Key(key string) xdelKey {
	return xdelKey{cs: append(c.cs, key)}
}

func (b *Builder) Xdel() (c xdel) {
	c.cs = append(b.get(), "XDEL")
	return
}

type xdelId struct {
	cs []string
}

func (c xdelId) Id(id ...string) xdelId {
	return xdelId{cs: append(c.cs, id...)}
}

func (c xdelId) Build() []string {
	return c.cs
}

type xdelKey struct {
	cs []string
}

func (c xdelKey) Id(id ...string) xdelId {
	return xdelId{cs: append(c.cs, id...)}
}

type xgroup struct {
	cs []string
}

func (c xgroup) Create(key string, groupname string) xgroupCreateCreate {
	return xgroupCreateCreate{cs: append(c.cs, "CREATE", key, groupname)}
}

func (c xgroup) Setid(key string, groupname string) xgroupSetidSetid {
	return xgroupSetidSetid{cs: append(c.cs, "SETID", key, groupname)}
}

func (c xgroup) Destroy(key string, groupname string) xgroupDestroy {
	return xgroupDestroy{cs: append(c.cs, "DESTROY", key, groupname)}
}

func (c xgroup) Createconsumer(key string, groupname string, consumername string) xgroupCreateconsumer {
	return xgroupCreateconsumer{cs: append(c.cs, "CREATECONSUMER", key, groupname, consumername)}
}

func (c xgroup) Delconsumer(key string, groupname string, consumername string) xgroupDelconsumer {
	return xgroupDelconsumer{cs: append(c.cs, "DELCONSUMER", key, groupname, consumername)}
}

func (b *Builder) Xgroup() (c xgroup) {
	c.cs = append(b.get(), "XGROUP")
	return
}

type xgroupCreateCreate struct {
	cs []string
}

func (c xgroupCreateCreate) Id() xgroupCreateIdId {
	return xgroupCreateIdId{cs: append(c.cs, "ID")}
}

func (c xgroupCreateCreate) Lastid() xgroupCreateIdLastID {
	return xgroupCreateIdLastID{cs: append(c.cs, "$")}
}

type xgroupCreateIdId struct {
	cs []string
}

func (c xgroupCreateIdId) Mkstream() xgroupCreateMkstream {
	return xgroupCreateMkstream{cs: append(c.cs, "MKSTREAM")}
}

func (c xgroupCreateIdId) Setid(key string, groupname string) xgroupSetidSetid {
	return xgroupSetidSetid{cs: append(c.cs, "SETID", key, groupname)}
}

func (c xgroupCreateIdId) Destroy(key string, groupname string) xgroupDestroy {
	return xgroupDestroy{cs: append(c.cs, "DESTROY", key, groupname)}
}

func (c xgroupCreateIdId) Createconsumer(key string, groupname string, consumername string) xgroupCreateconsumer {
	return xgroupCreateconsumer{cs: append(c.cs, "CREATECONSUMER", key, groupname, consumername)}
}

func (c xgroupCreateIdId) Delconsumer(key string, groupname string, consumername string) xgroupDelconsumer {
	return xgroupDelconsumer{cs: append(c.cs, "DELCONSUMER", key, groupname, consumername)}
}

type xgroupCreateIdLastID struct {
	cs []string
}

func (c xgroupCreateIdLastID) Mkstream() xgroupCreateMkstream {
	return xgroupCreateMkstream{cs: append(c.cs, "MKSTREAM")}
}

func (c xgroupCreateIdLastID) Setid(key string, groupname string) xgroupSetidSetid {
	return xgroupSetidSetid{cs: append(c.cs, "SETID", key, groupname)}
}

func (c xgroupCreateIdLastID) Destroy(key string, groupname string) xgroupDestroy {
	return xgroupDestroy{cs: append(c.cs, "DESTROY", key, groupname)}
}

func (c xgroupCreateIdLastID) Createconsumer(key string, groupname string, consumername string) xgroupCreateconsumer {
	return xgroupCreateconsumer{cs: append(c.cs, "CREATECONSUMER", key, groupname, consumername)}
}

func (c xgroupCreateIdLastID) Delconsumer(key string, groupname string, consumername string) xgroupDelconsumer {
	return xgroupDelconsumer{cs: append(c.cs, "DELCONSUMER", key, groupname, consumername)}
}

type xgroupCreateMkstream struct {
	cs []string
}

func (c xgroupCreateMkstream) Setid(key string, groupname string) xgroupSetidSetid {
	return xgroupSetidSetid{cs: append(c.cs, "SETID", key, groupname)}
}

func (c xgroupCreateMkstream) Destroy(key string, groupname string) xgroupDestroy {
	return xgroupDestroy{cs: append(c.cs, "DESTROY", key, groupname)}
}

func (c xgroupCreateMkstream) Createconsumer(key string, groupname string, consumername string) xgroupCreateconsumer {
	return xgroupCreateconsumer{cs: append(c.cs, "CREATECONSUMER", key, groupname, consumername)}
}

func (c xgroupCreateMkstream) Delconsumer(key string, groupname string, consumername string) xgroupDelconsumer {
	return xgroupDelconsumer{cs: append(c.cs, "DELCONSUMER", key, groupname, consumername)}
}

type xgroupCreateconsumer struct {
	cs []string
}

func (c xgroupCreateconsumer) Delconsumer(key string, groupname string, consumername string) xgroupDelconsumer {
	return xgroupDelconsumer{cs: append(c.cs, "DELCONSUMER", key, groupname, consumername)}
}

func (c xgroupCreateconsumer) Build() []string {
	return c.cs
}

type xgroupDelconsumer struct {
	cs []string
}

func (c xgroupDelconsumer) Build() []string {
	return c.cs
}

type xgroupDestroy struct {
	cs []string
}

func (c xgroupDestroy) Createconsumer(key string, groupname string, consumername string) xgroupCreateconsumer {
	return xgroupCreateconsumer{cs: append(c.cs, "CREATECONSUMER", key, groupname, consumername)}
}

func (c xgroupDestroy) Delconsumer(key string, groupname string, consumername string) xgroupDelconsumer {
	return xgroupDelconsumer{cs: append(c.cs, "DELCONSUMER", key, groupname, consumername)}
}

func (c xgroupDestroy) Build() []string {
	return c.cs
}

type xgroupSetidIdId struct {
	cs []string
}

func (c xgroupSetidIdId) Destroy(key string, groupname string) xgroupDestroy {
	return xgroupDestroy{cs: append(c.cs, "DESTROY", key, groupname)}
}

func (c xgroupSetidIdId) Createconsumer(key string, groupname string, consumername string) xgroupCreateconsumer {
	return xgroupCreateconsumer{cs: append(c.cs, "CREATECONSUMER", key, groupname, consumername)}
}

func (c xgroupSetidIdId) Delconsumer(key string, groupname string, consumername string) xgroupDelconsumer {
	return xgroupDelconsumer{cs: append(c.cs, "DELCONSUMER", key, groupname, consumername)}
}

func (c xgroupSetidIdId) Build() []string {
	return c.cs
}

type xgroupSetidIdLastID struct {
	cs []string
}

func (c xgroupSetidIdLastID) Destroy(key string, groupname string) xgroupDestroy {
	return xgroupDestroy{cs: append(c.cs, "DESTROY", key, groupname)}
}

func (c xgroupSetidIdLastID) Createconsumer(key string, groupname string, consumername string) xgroupCreateconsumer {
	return xgroupCreateconsumer{cs: append(c.cs, "CREATECONSUMER", key, groupname, consumername)}
}

func (c xgroupSetidIdLastID) Delconsumer(key string, groupname string, consumername string) xgroupDelconsumer {
	return xgroupDelconsumer{cs: append(c.cs, "DELCONSUMER", key, groupname, consumername)}
}

func (c xgroupSetidIdLastID) Build() []string {
	return c.cs
}

type xgroupSetidSetid struct {
	cs []string
}

func (c xgroupSetidSetid) Id() xgroupSetidIdId {
	return xgroupSetidIdId{cs: append(c.cs, "ID")}
}

func (c xgroupSetidSetid) Lastid() xgroupSetidIdLastID {
	return xgroupSetidIdLastID{cs: append(c.cs, "$")}
}

type xinfo struct {
	cs []string
}

func (c xinfo) Consumers(key string, groupname string) xinfoConsumers {
	return xinfoConsumers{cs: append(c.cs, "CONSUMERS", key, groupname)}
}

func (c xinfo) Groups(key string) xinfoGroups {
	return xinfoGroups{cs: append(c.cs, "GROUPS", key)}
}

func (c xinfo) Stream(key string) xinfoStream {
	return xinfoStream{cs: append(c.cs, "STREAM", key)}
}

func (c xinfo) Help() xinfoHelpHelp {
	return xinfoHelpHelp{cs: append(c.cs, "HELP")}
}

func (c xinfo) Build() []string {
	return c.cs
}

func (b *Builder) Xinfo() (c xinfo) {
	c.cs = append(b.get(), "XINFO")
	return
}

type xinfoConsumers struct {
	cs []string
}

func (c xinfoConsumers) Groups(key string) xinfoGroups {
	return xinfoGroups{cs: append(c.cs, "GROUPS", key)}
}

func (c xinfoConsumers) Stream(key string) xinfoStream {
	return xinfoStream{cs: append(c.cs, "STREAM", key)}
}

func (c xinfoConsumers) Help() xinfoHelpHelp {
	return xinfoHelpHelp{cs: append(c.cs, "HELP")}
}

func (c xinfoConsumers) Build() []string {
	return c.cs
}

type xinfoGroups struct {
	cs []string
}

func (c xinfoGroups) Stream(key string) xinfoStream {
	return xinfoStream{cs: append(c.cs, "STREAM", key)}
}

func (c xinfoGroups) Help() xinfoHelpHelp {
	return xinfoHelpHelp{cs: append(c.cs, "HELP")}
}

func (c xinfoGroups) Build() []string {
	return c.cs
}

type xinfoHelpHelp struct {
	cs []string
}

func (c xinfoHelpHelp) Build() []string {
	return c.cs
}

type xinfoStream struct {
	cs []string
}

func (c xinfoStream) Help() xinfoHelpHelp {
	return xinfoHelpHelp{cs: append(c.cs, "HELP")}
}

func (c xinfoStream) Build() []string {
	return c.cs
}

type xlen struct {
	cs []string
}

func (c xlen) Key(key string) xlenKey {
	return xlenKey{cs: append(c.cs, key)}
}

func (b *Builder) Xlen() (c xlen) {
	c.cs = append(b.get(), "XLEN")
	return
}

type xlenKey struct {
	cs []string
}

func (c xlenKey) Build() []string {
	return c.cs
}

type xpending struct {
	cs []string
}

func (c xpending) Key(key string) xpendingKey {
	return xpendingKey{cs: append(c.cs, key)}
}

func (b *Builder) Xpending() (c xpending) {
	c.cs = append(b.get(), "XPENDING")
	return
}

type xpendingFiltersConsumer struct {
	cs []string
}

func (c xpendingFiltersConsumer) Build() []string {
	return c.cs
}

type xpendingFiltersCount struct {
	cs []string
}

func (c xpendingFiltersCount) Consumer(consumer string) xpendingFiltersConsumer {
	return xpendingFiltersConsumer{cs: append(c.cs, consumer)}
}

func (c xpendingFiltersCount) Build() []string {
	return c.cs
}

type xpendingFiltersEnd struct {
	cs []string
}

func (c xpendingFiltersEnd) Count(count int64) xpendingFiltersCount {
	return xpendingFiltersCount{cs: append(c.cs, strconv.FormatInt(count, 10))}
}

type xpendingFiltersIdle struct {
	cs []string
}

func (c xpendingFiltersIdle) Start(start string) xpendingFiltersStart {
	return xpendingFiltersStart{cs: append(c.cs, start)}
}

type xpendingFiltersStart struct {
	cs []string
}

func (c xpendingFiltersStart) End(end string) xpendingFiltersEnd {
	return xpendingFiltersEnd{cs: append(c.cs, end)}
}

type xpendingGroup struct {
	cs []string
}

func (c xpendingGroup) Idle(minIdleTime int64) xpendingFiltersIdle {
	return xpendingFiltersIdle{cs: append(c.cs, "IDLE", strconv.FormatInt(minIdleTime, 10))}
}

func (c xpendingGroup) Start(start string) xpendingFiltersStart {
	return xpendingFiltersStart{cs: append(c.cs, start)}
}

type xpendingKey struct {
	cs []string
}

func (c xpendingKey) Group(group string) xpendingGroup {
	return xpendingGroup{cs: append(c.cs, group)}
}

type xrange struct {
	cs []string
}

func (c xrange) Key(key string) xrangeKey {
	return xrangeKey{cs: append(c.cs, key)}
}

func (b *Builder) Xrange() (c xrange) {
	c.cs = append(b.get(), "XRANGE")
	return
}

type xrangeCount struct {
	cs []string
}

func (c xrangeCount) Build() []string {
	return c.cs
}

type xrangeEnd struct {
	cs []string
}

func (c xrangeEnd) Count(count int64) xrangeCount {
	return xrangeCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c xrangeEnd) Build() []string {
	return c.cs
}

type xrangeKey struct {
	cs []string
}

func (c xrangeKey) Start(start string) xrangeStart {
	return xrangeStart{cs: append(c.cs, start)}
}

type xrangeStart struct {
	cs []string
}

func (c xrangeStart) End(end string) xrangeEnd {
	return xrangeEnd{cs: append(c.cs, end)}
}

type xread struct {
	cs []string
}

func (c xread) Count(count int64) xreadCount {
	return xreadCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c xread) Block(milliseconds int64) xreadBlock {
	return xreadBlock{cs: append(c.cs, "BLOCK", strconv.FormatInt(milliseconds, 10))}
}

func (c xread) Streams() xreadStreamsStreams {
	return xreadStreamsStreams{cs: append(c.cs, "STREAMS")}
}

func (b *Builder) Xread() (c xread) {
	c.cs = append(b.get(), "XREAD")
	return
}

type xreadBlock struct {
	cs []string
}

func (c xreadBlock) Streams() xreadStreamsStreams {
	return xreadStreamsStreams{cs: append(c.cs, "STREAMS")}
}

type xreadCount struct {
	cs []string
}

func (c xreadCount) Block(milliseconds int64) xreadBlock {
	return xreadBlock{cs: append(c.cs, "BLOCK", strconv.FormatInt(milliseconds, 10))}
}

func (c xreadCount) Streams() xreadStreamsStreams {
	return xreadStreamsStreams{cs: append(c.cs, "STREAMS")}
}

type xreadId struct {
	cs []string
}

func (c xreadId) Id(id ...string) xreadId {
	return xreadId{cs: append(c.cs, id...)}
}

func (c xreadId) Build() []string {
	return c.cs
}

type xreadKey struct {
	cs []string
}

func (c xreadKey) Id(id ...string) xreadId {
	return xreadId{cs: append(c.cs, id...)}
}

func (c xreadKey) Key(key ...string) xreadKey {
	return xreadKey{cs: append(c.cs, key...)}
}

type xreadStreamsStreams struct {
	cs []string
}

func (c xreadStreamsStreams) Key(key ...string) xreadKey {
	return xreadKey{cs: append(c.cs, key...)}
}

type xreadgroup struct {
	cs []string
}

func (c xreadgroup) Group(group string, consumer string) xreadgroupGroup {
	return xreadgroupGroup{cs: append(c.cs, "GROUP", group, consumer)}
}

func (b *Builder) Xreadgroup() (c xreadgroup) {
	c.cs = append(b.get(), "XREADGROUP")
	return
}

type xreadgroupBlock struct {
	cs []string
}

func (c xreadgroupBlock) Noack() xreadgroupNoackNoack {
	return xreadgroupNoackNoack{cs: append(c.cs, "NOACK")}
}

func (c xreadgroupBlock) Streams() xreadgroupStreamsStreams {
	return xreadgroupStreamsStreams{cs: append(c.cs, "STREAMS")}
}

type xreadgroupCount struct {
	cs []string
}

func (c xreadgroupCount) Block(milliseconds int64) xreadgroupBlock {
	return xreadgroupBlock{cs: append(c.cs, "BLOCK", strconv.FormatInt(milliseconds, 10))}
}

func (c xreadgroupCount) Noack() xreadgroupNoackNoack {
	return xreadgroupNoackNoack{cs: append(c.cs, "NOACK")}
}

func (c xreadgroupCount) Streams() xreadgroupStreamsStreams {
	return xreadgroupStreamsStreams{cs: append(c.cs, "STREAMS")}
}

type xreadgroupGroup struct {
	cs []string
}

func (c xreadgroupGroup) Count(count int64) xreadgroupCount {
	return xreadgroupCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c xreadgroupGroup) Block(milliseconds int64) xreadgroupBlock {
	return xreadgroupBlock{cs: append(c.cs, "BLOCK", strconv.FormatInt(milliseconds, 10))}
}

func (c xreadgroupGroup) Noack() xreadgroupNoackNoack {
	return xreadgroupNoackNoack{cs: append(c.cs, "NOACK")}
}

func (c xreadgroupGroup) Streams() xreadgroupStreamsStreams {
	return xreadgroupStreamsStreams{cs: append(c.cs, "STREAMS")}
}

type xreadgroupId struct {
	cs []string
}

func (c xreadgroupId) Id(id ...string) xreadgroupId {
	return xreadgroupId{cs: append(c.cs, id...)}
}

func (c xreadgroupId) Build() []string {
	return c.cs
}

type xreadgroupKey struct {
	cs []string
}

func (c xreadgroupKey) Id(id ...string) xreadgroupId {
	return xreadgroupId{cs: append(c.cs, id...)}
}

func (c xreadgroupKey) Key(key ...string) xreadgroupKey {
	return xreadgroupKey{cs: append(c.cs, key...)}
}

type xreadgroupNoackNoack struct {
	cs []string
}

func (c xreadgroupNoackNoack) Streams() xreadgroupStreamsStreams {
	return xreadgroupStreamsStreams{cs: append(c.cs, "STREAMS")}
}

type xreadgroupStreamsStreams struct {
	cs []string
}

func (c xreadgroupStreamsStreams) Key(key ...string) xreadgroupKey {
	return xreadgroupKey{cs: append(c.cs, key...)}
}

type xrevrange struct {
	cs []string
}

func (c xrevrange) Key(key string) xrevrangeKey {
	return xrevrangeKey{cs: append(c.cs, key)}
}

func (b *Builder) Xrevrange() (c xrevrange) {
	c.cs = append(b.get(), "XREVRANGE")
	return
}

type xrevrangeCount struct {
	cs []string
}

func (c xrevrangeCount) Build() []string {
	return c.cs
}

type xrevrangeEnd struct {
	cs []string
}

func (c xrevrangeEnd) Start(start string) xrevrangeStart {
	return xrevrangeStart{cs: append(c.cs, start)}
}

type xrevrangeKey struct {
	cs []string
}

func (c xrevrangeKey) End(end string) xrevrangeEnd {
	return xrevrangeEnd{cs: append(c.cs, end)}
}

type xrevrangeStart struct {
	cs []string
}

func (c xrevrangeStart) Count(count int64) xrevrangeCount {
	return xrevrangeCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c xrevrangeStart) Build() []string {
	return c.cs
}

type xtrim struct {
	cs []string
}

func (c xtrim) Key(key string) xtrimKey {
	return xtrimKey{cs: append(c.cs, key)}
}

func (b *Builder) Xtrim() (c xtrim) {
	c.cs = append(b.get(), "XTRIM")
	return
}

type xtrimKey struct {
	cs []string
}

func (c xtrimKey) Maxlen() xtrimTrimStrategyMaxlen {
	return xtrimTrimStrategyMaxlen{cs: append(c.cs, "MAXLEN")}
}

func (c xtrimKey) Minid() xtrimTrimStrategyMinid {
	return xtrimTrimStrategyMinid{cs: append(c.cs, "MINID")}
}

type xtrimTrimLimit struct {
	cs []string
}

func (c xtrimTrimLimit) Build() []string {
	return c.cs
}

type xtrimTrimOperatorAlmost struct {
	cs []string
}

func (c xtrimTrimOperatorAlmost) Threshold(threshold string) xtrimTrimThreshold {
	return xtrimTrimThreshold{cs: append(c.cs, threshold)}
}

type xtrimTrimOperatorExact struct {
	cs []string
}

func (c xtrimTrimOperatorExact) Threshold(threshold string) xtrimTrimThreshold {
	return xtrimTrimThreshold{cs: append(c.cs, threshold)}
}

type xtrimTrimStrategyMaxlen struct {
	cs []string
}

func (c xtrimTrimStrategyMaxlen) Exact() xtrimTrimOperatorExact {
	return xtrimTrimOperatorExact{cs: append(c.cs, "=")}
}

func (c xtrimTrimStrategyMaxlen) Almost() xtrimTrimOperatorAlmost {
	return xtrimTrimOperatorAlmost{cs: append(c.cs, "~")}
}

func (c xtrimTrimStrategyMaxlen) Threshold(threshold string) xtrimTrimThreshold {
	return xtrimTrimThreshold{cs: append(c.cs, threshold)}
}

type xtrimTrimStrategyMinid struct {
	cs []string
}

func (c xtrimTrimStrategyMinid) Exact() xtrimTrimOperatorExact {
	return xtrimTrimOperatorExact{cs: append(c.cs, "=")}
}

func (c xtrimTrimStrategyMinid) Almost() xtrimTrimOperatorAlmost {
	return xtrimTrimOperatorAlmost{cs: append(c.cs, "~")}
}

func (c xtrimTrimStrategyMinid) Threshold(threshold string) xtrimTrimThreshold {
	return xtrimTrimThreshold{cs: append(c.cs, threshold)}
}

type xtrimTrimThreshold struct {
	cs []string
}

func (c xtrimTrimThreshold) Limit(count int64) xtrimTrimLimit {
	return xtrimTrimLimit{cs: append(c.cs, "LIMIT", strconv.FormatInt(count, 10))}
}

func (c xtrimTrimThreshold) Build() []string {
	return c.cs
}

type zadd struct {
	cs []string
}

func (c zadd) Key(key string) zaddKey {
	return zaddKey{cs: append(c.cs, key)}
}

func (b *Builder) Zadd() (c zadd) {
	c.cs = append(b.get(), "ZADD")
	return
}

type zaddChangeCh struct {
	cs []string
}

func (c zaddChangeCh) Incr() zaddIncrementIncr {
	return zaddIncrementIncr{cs: append(c.cs, "INCR")}
}

func (c zaddChangeCh) ScoreMember() zaddScoreMember {
	return zaddScoreMember{cs: append(c.cs)}
}

type zaddComparisonGt struct {
	cs []string
}

func (c zaddComparisonGt) Ch() zaddChangeCh {
	return zaddChangeCh{cs: append(c.cs, "CH")}
}

func (c zaddComparisonGt) Incr() zaddIncrementIncr {
	return zaddIncrementIncr{cs: append(c.cs, "INCR")}
}

func (c zaddComparisonGt) ScoreMember() zaddScoreMember {
	return zaddScoreMember{cs: append(c.cs)}
}

type zaddComparisonLt struct {
	cs []string
}

func (c zaddComparisonLt) Ch() zaddChangeCh {
	return zaddChangeCh{cs: append(c.cs, "CH")}
}

func (c zaddComparisonLt) Incr() zaddIncrementIncr {
	return zaddIncrementIncr{cs: append(c.cs, "INCR")}
}

func (c zaddComparisonLt) ScoreMember() zaddScoreMember {
	return zaddScoreMember{cs: append(c.cs)}
}

type zaddConditionNx struct {
	cs []string
}

func (c zaddConditionNx) Gt() zaddComparisonGt {
	return zaddComparisonGt{cs: append(c.cs, "GT")}
}

func (c zaddConditionNx) Lt() zaddComparisonLt {
	return zaddComparisonLt{cs: append(c.cs, "LT")}
}

func (c zaddConditionNx) Ch() zaddChangeCh {
	return zaddChangeCh{cs: append(c.cs, "CH")}
}

func (c zaddConditionNx) Incr() zaddIncrementIncr {
	return zaddIncrementIncr{cs: append(c.cs, "INCR")}
}

func (c zaddConditionNx) ScoreMember() zaddScoreMember {
	return zaddScoreMember{cs: append(c.cs)}
}

type zaddConditionXx struct {
	cs []string
}

func (c zaddConditionXx) Gt() zaddComparisonGt {
	return zaddComparisonGt{cs: append(c.cs, "GT")}
}

func (c zaddConditionXx) Lt() zaddComparisonLt {
	return zaddComparisonLt{cs: append(c.cs, "LT")}
}

func (c zaddConditionXx) Ch() zaddChangeCh {
	return zaddChangeCh{cs: append(c.cs, "CH")}
}

func (c zaddConditionXx) Incr() zaddIncrementIncr {
	return zaddIncrementIncr{cs: append(c.cs, "INCR")}
}

func (c zaddConditionXx) ScoreMember() zaddScoreMember {
	return zaddScoreMember{cs: append(c.cs)}
}

type zaddIncrementIncr struct {
	cs []string
}

func (c zaddIncrementIncr) ScoreMember() zaddScoreMember {
	return zaddScoreMember{cs: append(c.cs)}
}

type zaddKey struct {
	cs []string
}

func (c zaddKey) Nx() zaddConditionNx {
	return zaddConditionNx{cs: append(c.cs, "NX")}
}

func (c zaddKey) Xx() zaddConditionXx {
	return zaddConditionXx{cs: append(c.cs, "XX")}
}

func (c zaddKey) Gt() zaddComparisonGt {
	return zaddComparisonGt{cs: append(c.cs, "GT")}
}

func (c zaddKey) Lt() zaddComparisonLt {
	return zaddComparisonLt{cs: append(c.cs, "LT")}
}

func (c zaddKey) Ch() zaddChangeCh {
	return zaddChangeCh{cs: append(c.cs, "CH")}
}

func (c zaddKey) Incr() zaddIncrementIncr {
	return zaddIncrementIncr{cs: append(c.cs, "INCR")}
}

func (c zaddKey) ScoreMember() zaddScoreMember {
	return zaddScoreMember{cs: append(c.cs)}
}

type zaddScoreMember struct {
	cs []string
}

func (c zaddScoreMember) ScoreMember(score float64, member string) zaddScoreMember {
	return zaddScoreMember{cs: append(c.cs, strconv.FormatFloat(score, 'f', -1, 64), member)}
}

func (c zaddScoreMember) Build() []string {
	return c.cs
}

type zcard struct {
	cs []string
}

func (c zcard) Key(key string) zcardKey {
	return zcardKey{cs: append(c.cs, key)}
}

func (b *Builder) Zcard() (c zcard) {
	c.cs = append(b.get(), "ZCARD")
	return
}

type zcardKey struct {
	cs []string
}

func (c zcardKey) Build() []string {
	return c.cs
}

type zcount struct {
	cs []string
}

func (c zcount) Key(key string) zcountKey {
	return zcountKey{cs: append(c.cs, key)}
}

func (b *Builder) Zcount() (c zcount) {
	c.cs = append(b.get(), "ZCOUNT")
	return
}

type zcountKey struct {
	cs []string
}

func (c zcountKey) Min(min float64) zcountMin {
	return zcountMin{cs: append(c.cs, strconv.FormatFloat(min, 'f', -1, 64))}
}

type zcountMax struct {
	cs []string
}

func (c zcountMax) Build() []string {
	return c.cs
}

type zcountMin struct {
	cs []string
}

func (c zcountMin) Max(max float64) zcountMax {
	return zcountMax{cs: append(c.cs, strconv.FormatFloat(max, 'f', -1, 64))}
}

type zdiff struct {
	cs []string
}

func (c zdiff) Numkeys(numkeys int64) zdiffNumkeys {
	return zdiffNumkeys{cs: append(c.cs, strconv.FormatInt(numkeys, 10))}
}

func (b *Builder) Zdiff() (c zdiff) {
	c.cs = append(b.get(), "ZDIFF")
	return
}

type zdiffKey struct {
	cs []string
}

func (c zdiffKey) Withscores() zdiffWithscoresWithscores {
	return zdiffWithscoresWithscores{cs: append(c.cs, "WITHSCORES")}
}

func (c zdiffKey) Key(key ...string) zdiffKey {
	return zdiffKey{cs: append(c.cs, key...)}
}

func (c zdiffKey) Build() []string {
	return c.cs
}

type zdiffNumkeys struct {
	cs []string
}

func (c zdiffNumkeys) Key(key ...string) zdiffKey {
	return zdiffKey{cs: append(c.cs, key...)}
}

type zdiffWithscoresWithscores struct {
	cs []string
}

func (c zdiffWithscoresWithscores) Build() []string {
	return c.cs
}

type zdiffstore struct {
	cs []string
}

func (c zdiffstore) Destination(destination string) zdiffstoreDestination {
	return zdiffstoreDestination{cs: append(c.cs, destination)}
}

func (b *Builder) Zdiffstore() (c zdiffstore) {
	c.cs = append(b.get(), "ZDIFFSTORE")
	return
}

type zdiffstoreDestination struct {
	cs []string
}

func (c zdiffstoreDestination) Numkeys(numkeys int64) zdiffstoreNumkeys {
	return zdiffstoreNumkeys{cs: append(c.cs, strconv.FormatInt(numkeys, 10))}
}

type zdiffstoreKey struct {
	cs []string
}

func (c zdiffstoreKey) Key(key ...string) zdiffstoreKey {
	return zdiffstoreKey{cs: append(c.cs, key...)}
}

func (c zdiffstoreKey) Build() []string {
	return c.cs
}

type zdiffstoreNumkeys struct {
	cs []string
}

func (c zdiffstoreNumkeys) Key(key ...string) zdiffstoreKey {
	return zdiffstoreKey{cs: append(c.cs, key...)}
}

type zincrby struct {
	cs []string
}

func (c zincrby) Key(key string) zincrbyKey {
	return zincrbyKey{cs: append(c.cs, key)}
}

func (b *Builder) Zincrby() (c zincrby) {
	c.cs = append(b.get(), "ZINCRBY")
	return
}

type zincrbyIncrement struct {
	cs []string
}

func (c zincrbyIncrement) Member(member string) zincrbyMember {
	return zincrbyMember{cs: append(c.cs, member)}
}

type zincrbyKey struct {
	cs []string
}

func (c zincrbyKey) Increment(increment int64) zincrbyIncrement {
	return zincrbyIncrement{cs: append(c.cs, strconv.FormatInt(increment, 10))}
}

type zincrbyMember struct {
	cs []string
}

func (c zincrbyMember) Build() []string {
	return c.cs
}

type zinter struct {
	cs []string
}

func (c zinter) Numkeys(numkeys int64) zinterNumkeys {
	return zinterNumkeys{cs: append(c.cs, strconv.FormatInt(numkeys, 10))}
}

func (b *Builder) Zinter() (c zinter) {
	c.cs = append(b.get(), "ZINTER")
	return
}

type zinterAggregateMax struct {
	cs []string
}

func (c zinterAggregateMax) Withscores() zinterWithscoresWithscores {
	return zinterWithscoresWithscores{cs: append(c.cs, "WITHSCORES")}
}

func (c zinterAggregateMax) Build() []string {
	return c.cs
}

type zinterAggregateMin struct {
	cs []string
}

func (c zinterAggregateMin) Withscores() zinterWithscoresWithscores {
	return zinterWithscoresWithscores{cs: append(c.cs, "WITHSCORES")}
}

func (c zinterAggregateMin) Build() []string {
	return c.cs
}

type zinterAggregateSum struct {
	cs []string
}

func (c zinterAggregateSum) Withscores() zinterWithscoresWithscores {
	return zinterWithscoresWithscores{cs: append(c.cs, "WITHSCORES")}
}

func (c zinterAggregateSum) Build() []string {
	return c.cs
}

type zinterKey struct {
	cs []string
}

func (c zinterKey) Weights(weight ...int64) zinterWeights {
	c.cs = append(c.cs, "WEIGHTS")
	for _, n := range weight {
		c.cs = append(c.cs, strconv.FormatInt(n, 10))
	}
	return zinterWeights{cs: c.cs}
}

func (c zinterKey) Sum() zinterAggregateSum {
	return zinterAggregateSum{cs: append(c.cs, "SUM")}
}

func (c zinterKey) Min() zinterAggregateMin {
	return zinterAggregateMin{cs: append(c.cs, "MIN")}
}

func (c zinterKey) Max() zinterAggregateMax {
	return zinterAggregateMax{cs: append(c.cs, "MAX")}
}

func (c zinterKey) Withscores() zinterWithscoresWithscores {
	return zinterWithscoresWithscores{cs: append(c.cs, "WITHSCORES")}
}

func (c zinterKey) Key(key ...string) zinterKey {
	return zinterKey{cs: append(c.cs, key...)}
}

func (c zinterKey) Build() []string {
	return c.cs
}

type zinterNumkeys struct {
	cs []string
}

func (c zinterNumkeys) Key(key ...string) zinterKey {
	return zinterKey{cs: append(c.cs, key...)}
}

type zinterWeights struct {
	cs []string
}

func (c zinterWeights) Sum() zinterAggregateSum {
	return zinterAggregateSum{cs: append(c.cs, "SUM")}
}

func (c zinterWeights) Min() zinterAggregateMin {
	return zinterAggregateMin{cs: append(c.cs, "MIN")}
}

func (c zinterWeights) Max() zinterAggregateMax {
	return zinterAggregateMax{cs: append(c.cs, "MAX")}
}

func (c zinterWeights) Withscores() zinterWithscoresWithscores {
	return zinterWithscoresWithscores{cs: append(c.cs, "WITHSCORES")}
}

func (c zinterWeights) Weights(weights ...int64) zinterWeights {
	for _, n := range weights {
		c.cs = append(c.cs, strconv.FormatInt(n, 10))
	}
	return zinterWeights{cs: c.cs}
}

func (c zinterWeights) Build() []string {
	return c.cs
}

type zinterWithscoresWithscores struct {
	cs []string
}

func (c zinterWithscoresWithscores) Build() []string {
	return c.cs
}

type zintercard struct {
	cs []string
}

func (c zintercard) Numkeys(numkeys int64) zintercardNumkeys {
	return zintercardNumkeys{cs: append(c.cs, strconv.FormatInt(numkeys, 10))}
}

func (b *Builder) Zintercard() (c zintercard) {
	c.cs = append(b.get(), "ZINTERCARD")
	return
}

type zintercardKey struct {
	cs []string
}

func (c zintercardKey) Key(key ...string) zintercardKey {
	return zintercardKey{cs: append(c.cs, key...)}
}

func (c zintercardKey) Build() []string {
	return c.cs
}

type zintercardNumkeys struct {
	cs []string
}

func (c zintercardNumkeys) Key(key ...string) zintercardKey {
	return zintercardKey{cs: append(c.cs, key...)}
}

type zinterstore struct {
	cs []string
}

func (c zinterstore) Destination(destination string) zinterstoreDestination {
	return zinterstoreDestination{cs: append(c.cs, destination)}
}

func (b *Builder) Zinterstore() (c zinterstore) {
	c.cs = append(b.get(), "ZINTERSTORE")
	return
}

type zinterstoreAggregateMax struct {
	cs []string
}

func (c zinterstoreAggregateMax) Build() []string {
	return c.cs
}

type zinterstoreAggregateMin struct {
	cs []string
}

func (c zinterstoreAggregateMin) Build() []string {
	return c.cs
}

type zinterstoreAggregateSum struct {
	cs []string
}

func (c zinterstoreAggregateSum) Build() []string {
	return c.cs
}

type zinterstoreDestination struct {
	cs []string
}

func (c zinterstoreDestination) Numkeys(numkeys int64) zinterstoreNumkeys {
	return zinterstoreNumkeys{cs: append(c.cs, strconv.FormatInt(numkeys, 10))}
}

type zinterstoreKey struct {
	cs []string
}

func (c zinterstoreKey) Weights(weight ...int64) zinterstoreWeights {
	c.cs = append(c.cs, "WEIGHTS")
	for _, n := range weight {
		c.cs = append(c.cs, strconv.FormatInt(n, 10))
	}
	return zinterstoreWeights{cs: c.cs}
}

func (c zinterstoreKey) Sum() zinterstoreAggregateSum {
	return zinterstoreAggregateSum{cs: append(c.cs, "SUM")}
}

func (c zinterstoreKey) Min() zinterstoreAggregateMin {
	return zinterstoreAggregateMin{cs: append(c.cs, "MIN")}
}

func (c zinterstoreKey) Max() zinterstoreAggregateMax {
	return zinterstoreAggregateMax{cs: append(c.cs, "MAX")}
}

func (c zinterstoreKey) Key(key ...string) zinterstoreKey {
	return zinterstoreKey{cs: append(c.cs, key...)}
}

func (c zinterstoreKey) Build() []string {
	return c.cs
}

type zinterstoreNumkeys struct {
	cs []string
}

func (c zinterstoreNumkeys) Key(key ...string) zinterstoreKey {
	return zinterstoreKey{cs: append(c.cs, key...)}
}

type zinterstoreWeights struct {
	cs []string
}

func (c zinterstoreWeights) Sum() zinterstoreAggregateSum {
	return zinterstoreAggregateSum{cs: append(c.cs, "SUM")}
}

func (c zinterstoreWeights) Min() zinterstoreAggregateMin {
	return zinterstoreAggregateMin{cs: append(c.cs, "MIN")}
}

func (c zinterstoreWeights) Max() zinterstoreAggregateMax {
	return zinterstoreAggregateMax{cs: append(c.cs, "MAX")}
}

func (c zinterstoreWeights) Weights(weights ...int64) zinterstoreWeights {
	for _, n := range weights {
		c.cs = append(c.cs, strconv.FormatInt(n, 10))
	}
	return zinterstoreWeights{cs: c.cs}
}

func (c zinterstoreWeights) Build() []string {
	return c.cs
}

type zlexcount struct {
	cs []string
}

func (c zlexcount) Key(key string) zlexcountKey {
	return zlexcountKey{cs: append(c.cs, key)}
}

func (b *Builder) Zlexcount() (c zlexcount) {
	c.cs = append(b.get(), "ZLEXCOUNT")
	return
}

type zlexcountKey struct {
	cs []string
}

func (c zlexcountKey) Min(min string) zlexcountMin {
	return zlexcountMin{cs: append(c.cs, min)}
}

type zlexcountMax struct {
	cs []string
}

func (c zlexcountMax) Build() []string {
	return c.cs
}

type zlexcountMin struct {
	cs []string
}

func (c zlexcountMin) Max(max string) zlexcountMax {
	return zlexcountMax{cs: append(c.cs, max)}
}

type zmscore struct {
	cs []string
}

func (c zmscore) Key(key string) zmscoreKey {
	return zmscoreKey{cs: append(c.cs, key)}
}

func (b *Builder) Zmscore() (c zmscore) {
	c.cs = append(b.get(), "ZMSCORE")
	return
}

type zmscoreKey struct {
	cs []string
}

func (c zmscoreKey) Member(member ...string) zmscoreMember {
	return zmscoreMember{cs: append(c.cs, member...)}
}

type zmscoreMember struct {
	cs []string
}

func (c zmscoreMember) Member(member ...string) zmscoreMember {
	return zmscoreMember{cs: append(c.cs, member...)}
}

func (c zmscoreMember) Build() []string {
	return c.cs
}

type zpopmax struct {
	cs []string
}

func (c zpopmax) Key(key string) zpopmaxKey {
	return zpopmaxKey{cs: append(c.cs, key)}
}

func (b *Builder) Zpopmax() (c zpopmax) {
	c.cs = append(b.get(), "ZPOPMAX")
	return
}

type zpopmaxCount struct {
	cs []string
}

func (c zpopmaxCount) Build() []string {
	return c.cs
}

type zpopmaxKey struct {
	cs []string
}

func (c zpopmaxKey) Count(count int64) zpopmaxCount {
	return zpopmaxCount{cs: append(c.cs, strconv.FormatInt(count, 10))}
}

func (c zpopmaxKey) Build() []string {
	return c.cs
}

type zpopmin struct {
	cs []string
}

func (c zpopmin) Key(key string) zpopminKey {
	return zpopminKey{cs: append(c.cs, key)}
}

func (b *Builder) Zpopmin() (c zpopmin) {
	c.cs = append(b.get(), "ZPOPMIN")
	return
}

type zpopminCount struct {
	cs []string
}

func (c zpopminCount) Build() []string {
	return c.cs
}

type zpopminKey struct {
	cs []string
}

func (c zpopminKey) Count(count int64) zpopminCount {
	return zpopminCount{cs: append(c.cs, strconv.FormatInt(count, 10))}
}

func (c zpopminKey) Build() []string {
	return c.cs
}

type zrandmember struct {
	cs []string
}

func (c zrandmember) Key(key string) zrandmemberKey {
	return zrandmemberKey{cs: append(c.cs, key)}
}

func (b *Builder) Zrandmember() (c zrandmember) {
	c.cs = append(b.get(), "ZRANDMEMBER")
	return
}

type zrandmemberKey struct {
	cs []string
}

func (c zrandmemberKey) Count(count int64) zrandmemberOptionsCount {
	return zrandmemberOptionsCount{cs: append(c.cs, strconv.FormatInt(count, 10))}
}

type zrandmemberOptionsCount struct {
	cs []string
}

func (c zrandmemberOptionsCount) Withscores() zrandmemberOptionsWithscoresWithscores {
	return zrandmemberOptionsWithscoresWithscores{cs: append(c.cs, "WITHSCORES")}
}

func (c zrandmemberOptionsCount) Build() []string {
	return c.cs
}

type zrandmemberOptionsWithscoresWithscores struct {
	cs []string
}

func (c zrandmemberOptionsWithscoresWithscores) Build() []string {
	return c.cs
}

type zrange struct {
	cs []string
}

func (c zrange) Key(key string) zrangeKey {
	return zrangeKey{cs: append(c.cs, key)}
}

func (b *Builder) Zrange() (c zrange) {
	c.cs = append(b.get(), "ZRANGE")
	return
}

type zrangeKey struct {
	cs []string
}

func (c zrangeKey) Min(min string) zrangeMin {
	return zrangeMin{cs: append(c.cs, min)}
}

type zrangeLimit struct {
	cs []string
}

func (c zrangeLimit) Withscores() zrangeWithscoresWithscores {
	return zrangeWithscoresWithscores{cs: append(c.cs, "WITHSCORES")}
}

func (c zrangeLimit) Build() []string {
	return c.cs
}

type zrangeMax struct {
	cs []string
}

func (c zrangeMax) Byscore() zrangeSortbyByscore {
	return zrangeSortbyByscore{cs: append(c.cs, "BYSCORE")}
}

func (c zrangeMax) Bylex() zrangeSortbyBylex {
	return zrangeSortbyBylex{cs: append(c.cs, "BYLEX")}
}

func (c zrangeMax) Rev() zrangeRevRev {
	return zrangeRevRev{cs: append(c.cs, "REV")}
}

func (c zrangeMax) Limit(offset int64, count int64) zrangeLimit {
	return zrangeLimit{cs: append(c.cs, "LIMIT", strconv.FormatInt(offset, 10), strconv.FormatInt(count, 10))}
}

func (c zrangeMax) Withscores() zrangeWithscoresWithscores {
	return zrangeWithscoresWithscores{cs: append(c.cs, "WITHSCORES")}
}

func (c zrangeMax) Build() []string {
	return c.cs
}

type zrangeMin struct {
	cs []string
}

func (c zrangeMin) Max(max string) zrangeMax {
	return zrangeMax{cs: append(c.cs, max)}
}

type zrangeRevRev struct {
	cs []string
}

func (c zrangeRevRev) Limit(offset int64, count int64) zrangeLimit {
	return zrangeLimit{cs: append(c.cs, "LIMIT", strconv.FormatInt(offset, 10), strconv.FormatInt(count, 10))}
}

func (c zrangeRevRev) Withscores() zrangeWithscoresWithscores {
	return zrangeWithscoresWithscores{cs: append(c.cs, "WITHSCORES")}
}

func (c zrangeRevRev) Build() []string {
	return c.cs
}

type zrangeSortbyBylex struct {
	cs []string
}

func (c zrangeSortbyBylex) Rev() zrangeRevRev {
	return zrangeRevRev{cs: append(c.cs, "REV")}
}

func (c zrangeSortbyBylex) Limit(offset int64, count int64) zrangeLimit {
	return zrangeLimit{cs: append(c.cs, "LIMIT", strconv.FormatInt(offset, 10), strconv.FormatInt(count, 10))}
}

func (c zrangeSortbyBylex) Withscores() zrangeWithscoresWithscores {
	return zrangeWithscoresWithscores{cs: append(c.cs, "WITHSCORES")}
}

func (c zrangeSortbyBylex) Build() []string {
	return c.cs
}

type zrangeSortbyByscore struct {
	cs []string
}

func (c zrangeSortbyByscore) Rev() zrangeRevRev {
	return zrangeRevRev{cs: append(c.cs, "REV")}
}

func (c zrangeSortbyByscore) Limit(offset int64, count int64) zrangeLimit {
	return zrangeLimit{cs: append(c.cs, "LIMIT", strconv.FormatInt(offset, 10), strconv.FormatInt(count, 10))}
}

func (c zrangeSortbyByscore) Withscores() zrangeWithscoresWithscores {
	return zrangeWithscoresWithscores{cs: append(c.cs, "WITHSCORES")}
}

func (c zrangeSortbyByscore) Build() []string {
	return c.cs
}

type zrangeWithscoresWithscores struct {
	cs []string
}

func (c zrangeWithscoresWithscores) Build() []string {
	return c.cs
}

type zrangebylex struct {
	cs []string
}

func (c zrangebylex) Key(key string) zrangebylexKey {
	return zrangebylexKey{cs: append(c.cs, key)}
}

func (b *Builder) Zrangebylex() (c zrangebylex) {
	c.cs = append(b.get(), "ZRANGEBYLEX")
	return
}

type zrangebylexKey struct {
	cs []string
}

func (c zrangebylexKey) Min(min string) zrangebylexMin {
	return zrangebylexMin{cs: append(c.cs, min)}
}

type zrangebylexLimit struct {
	cs []string
}

func (c zrangebylexLimit) Build() []string {
	return c.cs
}

type zrangebylexMax struct {
	cs []string
}

func (c zrangebylexMax) Limit(offset int64, count int64) zrangebylexLimit {
	return zrangebylexLimit{cs: append(c.cs, "LIMIT", strconv.FormatInt(offset, 10), strconv.FormatInt(count, 10))}
}

func (c zrangebylexMax) Build() []string {
	return c.cs
}

type zrangebylexMin struct {
	cs []string
}

func (c zrangebylexMin) Max(max string) zrangebylexMax {
	return zrangebylexMax{cs: append(c.cs, max)}
}

type zrangebyscore struct {
	cs []string
}

func (c zrangebyscore) Key(key string) zrangebyscoreKey {
	return zrangebyscoreKey{cs: append(c.cs, key)}
}

func (b *Builder) Zrangebyscore() (c zrangebyscore) {
	c.cs = append(b.get(), "ZRANGEBYSCORE")
	return
}

type zrangebyscoreKey struct {
	cs []string
}

func (c zrangebyscoreKey) Min(min float64) zrangebyscoreMin {
	return zrangebyscoreMin{cs: append(c.cs, strconv.FormatFloat(min, 'f', -1, 64))}
}

type zrangebyscoreLimit struct {
	cs []string
}

func (c zrangebyscoreLimit) Build() []string {
	return c.cs
}

type zrangebyscoreMax struct {
	cs []string
}

func (c zrangebyscoreMax) Withscores() zrangebyscoreWithscoresWithscores {
	return zrangebyscoreWithscoresWithscores{cs: append(c.cs, "WITHSCORES")}
}

func (c zrangebyscoreMax) Limit(offset int64, count int64) zrangebyscoreLimit {
	return zrangebyscoreLimit{cs: append(c.cs, "LIMIT", strconv.FormatInt(offset, 10), strconv.FormatInt(count, 10))}
}

func (c zrangebyscoreMax) Build() []string {
	return c.cs
}

type zrangebyscoreMin struct {
	cs []string
}

func (c zrangebyscoreMin) Max(max float64) zrangebyscoreMax {
	return zrangebyscoreMax{cs: append(c.cs, strconv.FormatFloat(max, 'f', -1, 64))}
}

type zrangebyscoreWithscoresWithscores struct {
	cs []string
}

func (c zrangebyscoreWithscoresWithscores) Limit(offset int64, count int64) zrangebyscoreLimit {
	return zrangebyscoreLimit{cs: append(c.cs, "LIMIT", strconv.FormatInt(offset, 10), strconv.FormatInt(count, 10))}
}

func (c zrangebyscoreWithscoresWithscores) Build() []string {
	return c.cs
}

type zrangestore struct {
	cs []string
}

func (c zrangestore) Dst(dst string) zrangestoreDst {
	return zrangestoreDst{cs: append(c.cs, dst)}
}

func (b *Builder) Zrangestore() (c zrangestore) {
	c.cs = append(b.get(), "ZRANGESTORE")
	return
}

type zrangestoreDst struct {
	cs []string
}

func (c zrangestoreDst) Src(src string) zrangestoreSrc {
	return zrangestoreSrc{cs: append(c.cs, src)}
}

type zrangestoreLimit struct {
	cs []string
}

func (c zrangestoreLimit) Build() []string {
	return c.cs
}

type zrangestoreMax struct {
	cs []string
}

func (c zrangestoreMax) Byscore() zrangestoreSortbyByscore {
	return zrangestoreSortbyByscore{cs: append(c.cs, "BYSCORE")}
}

func (c zrangestoreMax) Bylex() zrangestoreSortbyBylex {
	return zrangestoreSortbyBylex{cs: append(c.cs, "BYLEX")}
}

func (c zrangestoreMax) Rev() zrangestoreRevRev {
	return zrangestoreRevRev{cs: append(c.cs, "REV")}
}

func (c zrangestoreMax) Limit(offset int64, count int64) zrangestoreLimit {
	return zrangestoreLimit{cs: append(c.cs, "LIMIT", strconv.FormatInt(offset, 10), strconv.FormatInt(count, 10))}
}

func (c zrangestoreMax) Build() []string {
	return c.cs
}

type zrangestoreMin struct {
	cs []string
}

func (c zrangestoreMin) Max(max string) zrangestoreMax {
	return zrangestoreMax{cs: append(c.cs, max)}
}

type zrangestoreRevRev struct {
	cs []string
}

func (c zrangestoreRevRev) Limit(offset int64, count int64) zrangestoreLimit {
	return zrangestoreLimit{cs: append(c.cs, "LIMIT", strconv.FormatInt(offset, 10), strconv.FormatInt(count, 10))}
}

func (c zrangestoreRevRev) Build() []string {
	return c.cs
}

type zrangestoreSortbyBylex struct {
	cs []string
}

func (c zrangestoreSortbyBylex) Rev() zrangestoreRevRev {
	return zrangestoreRevRev{cs: append(c.cs, "REV")}
}

func (c zrangestoreSortbyBylex) Limit(offset int64, count int64) zrangestoreLimit {
	return zrangestoreLimit{cs: append(c.cs, "LIMIT", strconv.FormatInt(offset, 10), strconv.FormatInt(count, 10))}
}

func (c zrangestoreSortbyBylex) Build() []string {
	return c.cs
}

type zrangestoreSortbyByscore struct {
	cs []string
}

func (c zrangestoreSortbyByscore) Rev() zrangestoreRevRev {
	return zrangestoreRevRev{cs: append(c.cs, "REV")}
}

func (c zrangestoreSortbyByscore) Limit(offset int64, count int64) zrangestoreLimit {
	return zrangestoreLimit{cs: append(c.cs, "LIMIT", strconv.FormatInt(offset, 10), strconv.FormatInt(count, 10))}
}

func (c zrangestoreSortbyByscore) Build() []string {
	return c.cs
}

type zrangestoreSrc struct {
	cs []string
}

func (c zrangestoreSrc) Min(min string) zrangestoreMin {
	return zrangestoreMin{cs: append(c.cs, min)}
}

type zrank struct {
	cs []string
}

func (c zrank) Key(key string) zrankKey {
	return zrankKey{cs: append(c.cs, key)}
}

func (b *Builder) Zrank() (c zrank) {
	c.cs = append(b.get(), "ZRANK")
	return
}

type zrankKey struct {
	cs []string
}

func (c zrankKey) Member(member string) zrankMember {
	return zrankMember{cs: append(c.cs, member)}
}

type zrankMember struct {
	cs []string
}

func (c zrankMember) Build() []string {
	return c.cs
}

type zrem struct {
	cs []string
}

func (c zrem) Key(key string) zremKey {
	return zremKey{cs: append(c.cs, key)}
}

func (b *Builder) Zrem() (c zrem) {
	c.cs = append(b.get(), "ZREM")
	return
}

type zremKey struct {
	cs []string
}

func (c zremKey) Member(member ...string) zremMember {
	return zremMember{cs: append(c.cs, member...)}
}

type zremMember struct {
	cs []string
}

func (c zremMember) Member(member ...string) zremMember {
	return zremMember{cs: append(c.cs, member...)}
}

func (c zremMember) Build() []string {
	return c.cs
}

type zremrangebylex struct {
	cs []string
}

func (c zremrangebylex) Key(key string) zremrangebylexKey {
	return zremrangebylexKey{cs: append(c.cs, key)}
}

func (b *Builder) Zremrangebylex() (c zremrangebylex) {
	c.cs = append(b.get(), "ZREMRANGEBYLEX")
	return
}

type zremrangebylexKey struct {
	cs []string
}

func (c zremrangebylexKey) Min(min string) zremrangebylexMin {
	return zremrangebylexMin{cs: append(c.cs, min)}
}

type zremrangebylexMax struct {
	cs []string
}

func (c zremrangebylexMax) Build() []string {
	return c.cs
}

type zremrangebylexMin struct {
	cs []string
}

func (c zremrangebylexMin) Max(max string) zremrangebylexMax {
	return zremrangebylexMax{cs: append(c.cs, max)}
}

type zremrangebyrank struct {
	cs []string
}

func (c zremrangebyrank) Key(key string) zremrangebyrankKey {
	return zremrangebyrankKey{cs: append(c.cs, key)}
}

func (b *Builder) Zremrangebyrank() (c zremrangebyrank) {
	c.cs = append(b.get(), "ZREMRANGEBYRANK")
	return
}

type zremrangebyrankKey struct {
	cs []string
}

func (c zremrangebyrankKey) Start(start int64) zremrangebyrankStart {
	return zremrangebyrankStart{cs: append(c.cs, strconv.FormatInt(start, 10))}
}

type zremrangebyrankStart struct {
	cs []string
}

func (c zremrangebyrankStart) Stop(stop int64) zremrangebyrankStop {
	return zremrangebyrankStop{cs: append(c.cs, strconv.FormatInt(stop, 10))}
}

type zremrangebyrankStop struct {
	cs []string
}

func (c zremrangebyrankStop) Build() []string {
	return c.cs
}

type zremrangebyscore struct {
	cs []string
}

func (c zremrangebyscore) Key(key string) zremrangebyscoreKey {
	return zremrangebyscoreKey{cs: append(c.cs, key)}
}

func (b *Builder) Zremrangebyscore() (c zremrangebyscore) {
	c.cs = append(b.get(), "ZREMRANGEBYSCORE")
	return
}

type zremrangebyscoreKey struct {
	cs []string
}

func (c zremrangebyscoreKey) Min(min float64) zremrangebyscoreMin {
	return zremrangebyscoreMin{cs: append(c.cs, strconv.FormatFloat(min, 'f', -1, 64))}
}

type zremrangebyscoreMax struct {
	cs []string
}

func (c zremrangebyscoreMax) Build() []string {
	return c.cs
}

type zremrangebyscoreMin struct {
	cs []string
}

func (c zremrangebyscoreMin) Max(max float64) zremrangebyscoreMax {
	return zremrangebyscoreMax{cs: append(c.cs, strconv.FormatFloat(max, 'f', -1, 64))}
}

type zrevrange struct {
	cs []string
}

func (c zrevrange) Key(key string) zrevrangeKey {
	return zrevrangeKey{cs: append(c.cs, key)}
}

func (b *Builder) Zrevrange() (c zrevrange) {
	c.cs = append(b.get(), "ZREVRANGE")
	return
}

type zrevrangeKey struct {
	cs []string
}

func (c zrevrangeKey) Start(start int64) zrevrangeStart {
	return zrevrangeStart{cs: append(c.cs, strconv.FormatInt(start, 10))}
}

type zrevrangeStart struct {
	cs []string
}

func (c zrevrangeStart) Stop(stop int64) zrevrangeStop {
	return zrevrangeStop{cs: append(c.cs, strconv.FormatInt(stop, 10))}
}

type zrevrangeStop struct {
	cs []string
}

func (c zrevrangeStop) Withscores() zrevrangeWithscoresWithscores {
	return zrevrangeWithscoresWithscores{cs: append(c.cs, "WITHSCORES")}
}

func (c zrevrangeStop) Build() []string {
	return c.cs
}

type zrevrangeWithscoresWithscores struct {
	cs []string
}

func (c zrevrangeWithscoresWithscores) Build() []string {
	return c.cs
}

type zrevrangebylex struct {
	cs []string
}

func (c zrevrangebylex) Key(key string) zrevrangebylexKey {
	return zrevrangebylexKey{cs: append(c.cs, key)}
}

func (b *Builder) Zrevrangebylex() (c zrevrangebylex) {
	c.cs = append(b.get(), "ZREVRANGEBYLEX")
	return
}

type zrevrangebylexKey struct {
	cs []string
}

func (c zrevrangebylexKey) Max(max string) zrevrangebylexMax {
	return zrevrangebylexMax{cs: append(c.cs, max)}
}

type zrevrangebylexLimit struct {
	cs []string
}

func (c zrevrangebylexLimit) Build() []string {
	return c.cs
}

type zrevrangebylexMax struct {
	cs []string
}

func (c zrevrangebylexMax) Min(min string) zrevrangebylexMin {
	return zrevrangebylexMin{cs: append(c.cs, min)}
}

type zrevrangebylexMin struct {
	cs []string
}

func (c zrevrangebylexMin) Limit(offset int64, count int64) zrevrangebylexLimit {
	return zrevrangebylexLimit{cs: append(c.cs, "LIMIT", strconv.FormatInt(offset, 10), strconv.FormatInt(count, 10))}
}

func (c zrevrangebylexMin) Build() []string {
	return c.cs
}

type zrevrangebyscore struct {
	cs []string
}

func (c zrevrangebyscore) Key(key string) zrevrangebyscoreKey {
	return zrevrangebyscoreKey{cs: append(c.cs, key)}
}

func (b *Builder) Zrevrangebyscore() (c zrevrangebyscore) {
	c.cs = append(b.get(), "ZREVRANGEBYSCORE")
	return
}

type zrevrangebyscoreKey struct {
	cs []string
}

func (c zrevrangebyscoreKey) Max(max float64) zrevrangebyscoreMax {
	return zrevrangebyscoreMax{cs: append(c.cs, strconv.FormatFloat(max, 'f', -1, 64))}
}

type zrevrangebyscoreLimit struct {
	cs []string
}

func (c zrevrangebyscoreLimit) Build() []string {
	return c.cs
}

type zrevrangebyscoreMax struct {
	cs []string
}

func (c zrevrangebyscoreMax) Min(min float64) zrevrangebyscoreMin {
	return zrevrangebyscoreMin{cs: append(c.cs, strconv.FormatFloat(min, 'f', -1, 64))}
}

type zrevrangebyscoreMin struct {
	cs []string
}

func (c zrevrangebyscoreMin) Withscores() zrevrangebyscoreWithscoresWithscores {
	return zrevrangebyscoreWithscoresWithscores{cs: append(c.cs, "WITHSCORES")}
}

func (c zrevrangebyscoreMin) Limit(offset int64, count int64) zrevrangebyscoreLimit {
	return zrevrangebyscoreLimit{cs: append(c.cs, "LIMIT", strconv.FormatInt(offset, 10), strconv.FormatInt(count, 10))}
}

func (c zrevrangebyscoreMin) Build() []string {
	return c.cs
}

type zrevrangebyscoreWithscoresWithscores struct {
	cs []string
}

func (c zrevrangebyscoreWithscoresWithscores) Limit(offset int64, count int64) zrevrangebyscoreLimit {
	return zrevrangebyscoreLimit{cs: append(c.cs, "LIMIT", strconv.FormatInt(offset, 10), strconv.FormatInt(count, 10))}
}

func (c zrevrangebyscoreWithscoresWithscores) Build() []string {
	return c.cs
}

type zrevrank struct {
	cs []string
}

func (c zrevrank) Key(key string) zrevrankKey {
	return zrevrankKey{cs: append(c.cs, key)}
}

func (b *Builder) Zrevrank() (c zrevrank) {
	c.cs = append(b.get(), "ZREVRANK")
	return
}

type zrevrankKey struct {
	cs []string
}

func (c zrevrankKey) Member(member string) zrevrankMember {
	return zrevrankMember{cs: append(c.cs, member)}
}

type zrevrankMember struct {
	cs []string
}

func (c zrevrankMember) Build() []string {
	return c.cs
}

type zscan struct {
	cs []string
}

func (c zscan) Key(key string) zscanKey {
	return zscanKey{cs: append(c.cs, key)}
}

func (b *Builder) Zscan() (c zscan) {
	c.cs = append(b.get(), "ZSCAN")
	return
}

type zscanCount struct {
	cs []string
}

func (c zscanCount) Build() []string {
	return c.cs
}

type zscanCursor struct {
	cs []string
}

func (c zscanCursor) Match(pattern string) zscanMatch {
	return zscanMatch{cs: append(c.cs, "MATCH", pattern)}
}

func (c zscanCursor) Count(count int64) zscanCount {
	return zscanCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c zscanCursor) Build() []string {
	return c.cs
}

type zscanKey struct {
	cs []string
}

func (c zscanKey) Cursor(cursor int64) zscanCursor {
	return zscanCursor{cs: append(c.cs, strconv.FormatInt(cursor, 10))}
}

type zscanMatch struct {
	cs []string
}

func (c zscanMatch) Count(count int64) zscanCount {
	return zscanCount{cs: append(c.cs, "COUNT", strconv.FormatInt(count, 10))}
}

func (c zscanMatch) Build() []string {
	return c.cs
}

type zscore struct {
	cs []string
}

func (c zscore) Key(key string) zscoreKey {
	return zscoreKey{cs: append(c.cs, key)}
}

func (b *Builder) Zscore() (c zscore) {
	c.cs = append(b.get(), "ZSCORE")
	return
}

type zscoreKey struct {
	cs []string
}

func (c zscoreKey) Member(member string) zscoreMember {
	return zscoreMember{cs: append(c.cs, member)}
}

type zscoreMember struct {
	cs []string
}

func (c zscoreMember) Build() []string {
	return c.cs
}

type zunion struct {
	cs []string
}

func (c zunion) Numkeys(numkeys int64) zunionNumkeys {
	return zunionNumkeys{cs: append(c.cs, strconv.FormatInt(numkeys, 10))}
}

func (b *Builder) Zunion() (c zunion) {
	c.cs = append(b.get(), "ZUNION")
	return
}

type zunionAggregateMax struct {
	cs []string
}

func (c zunionAggregateMax) Withscores() zunionWithscoresWithscores {
	return zunionWithscoresWithscores{cs: append(c.cs, "WITHSCORES")}
}

func (c zunionAggregateMax) Build() []string {
	return c.cs
}

type zunionAggregateMin struct {
	cs []string
}

func (c zunionAggregateMin) Withscores() zunionWithscoresWithscores {
	return zunionWithscoresWithscores{cs: append(c.cs, "WITHSCORES")}
}

func (c zunionAggregateMin) Build() []string {
	return c.cs
}

type zunionAggregateSum struct {
	cs []string
}

func (c zunionAggregateSum) Withscores() zunionWithscoresWithscores {
	return zunionWithscoresWithscores{cs: append(c.cs, "WITHSCORES")}
}

func (c zunionAggregateSum) Build() []string {
	return c.cs
}

type zunionKey struct {
	cs []string
}

func (c zunionKey) Weights(weight ...int64) zunionWeights {
	c.cs = append(c.cs, "WEIGHTS")
	for _, n := range weight {
		c.cs = append(c.cs, strconv.FormatInt(n, 10))
	}
	return zunionWeights{cs: c.cs}
}

func (c zunionKey) Sum() zunionAggregateSum {
	return zunionAggregateSum{cs: append(c.cs, "SUM")}
}

func (c zunionKey) Min() zunionAggregateMin {
	return zunionAggregateMin{cs: append(c.cs, "MIN")}
}

func (c zunionKey) Max() zunionAggregateMax {
	return zunionAggregateMax{cs: append(c.cs, "MAX")}
}

func (c zunionKey) Withscores() zunionWithscoresWithscores {
	return zunionWithscoresWithscores{cs: append(c.cs, "WITHSCORES")}
}

func (c zunionKey) Key(key ...string) zunionKey {
	return zunionKey{cs: append(c.cs, key...)}
}

func (c zunionKey) Build() []string {
	return c.cs
}

type zunionNumkeys struct {
	cs []string
}

func (c zunionNumkeys) Key(key ...string) zunionKey {
	return zunionKey{cs: append(c.cs, key...)}
}

type zunionWeights struct {
	cs []string
}

func (c zunionWeights) Sum() zunionAggregateSum {
	return zunionAggregateSum{cs: append(c.cs, "SUM")}
}

func (c zunionWeights) Min() zunionAggregateMin {
	return zunionAggregateMin{cs: append(c.cs, "MIN")}
}

func (c zunionWeights) Max() zunionAggregateMax {
	return zunionAggregateMax{cs: append(c.cs, "MAX")}
}

func (c zunionWeights) Withscores() zunionWithscoresWithscores {
	return zunionWithscoresWithscores{cs: append(c.cs, "WITHSCORES")}
}

func (c zunionWeights) Weights(weights ...int64) zunionWeights {
	for _, n := range weights {
		c.cs = append(c.cs, strconv.FormatInt(n, 10))
	}
	return zunionWeights{cs: c.cs}
}

func (c zunionWeights) Build() []string {
	return c.cs
}

type zunionWithscoresWithscores struct {
	cs []string
}

func (c zunionWithscoresWithscores) Build() []string {
	return c.cs
}

type zunionstore struct {
	cs []string
}

func (c zunionstore) Destination(destination string) zunionstoreDestination {
	return zunionstoreDestination{cs: append(c.cs, destination)}
}

func (b *Builder) Zunionstore() (c zunionstore) {
	c.cs = append(b.get(), "ZUNIONSTORE")
	return
}

type zunionstoreAggregateMax struct {
	cs []string
}

func (c zunionstoreAggregateMax) Build() []string {
	return c.cs
}

type zunionstoreAggregateMin struct {
	cs []string
}

func (c zunionstoreAggregateMin) Build() []string {
	return c.cs
}

type zunionstoreAggregateSum struct {
	cs []string
}

func (c zunionstoreAggregateSum) Build() []string {
	return c.cs
}

type zunionstoreDestination struct {
	cs []string
}

func (c zunionstoreDestination) Numkeys(numkeys int64) zunionstoreNumkeys {
	return zunionstoreNumkeys{cs: append(c.cs, strconv.FormatInt(numkeys, 10))}
}

type zunionstoreKey struct {
	cs []string
}

func (c zunionstoreKey) Weights(weight ...int64) zunionstoreWeights {
	c.cs = append(c.cs, "WEIGHTS")
	for _, n := range weight {
		c.cs = append(c.cs, strconv.FormatInt(n, 10))
	}
	return zunionstoreWeights{cs: c.cs}
}

func (c zunionstoreKey) Sum() zunionstoreAggregateSum {
	return zunionstoreAggregateSum{cs: append(c.cs, "SUM")}
}

func (c zunionstoreKey) Min() zunionstoreAggregateMin {
	return zunionstoreAggregateMin{cs: append(c.cs, "MIN")}
}

func (c zunionstoreKey) Max() zunionstoreAggregateMax {
	return zunionstoreAggregateMax{cs: append(c.cs, "MAX")}
}

func (c zunionstoreKey) Key(key ...string) zunionstoreKey {
	return zunionstoreKey{cs: append(c.cs, key...)}
}

func (c zunionstoreKey) Build() []string {
	return c.cs
}

type zunionstoreNumkeys struct {
	cs []string
}

func (c zunionstoreNumkeys) Key(key ...string) zunionstoreKey {
	return zunionstoreKey{cs: append(c.cs, key...)}
}

type zunionstoreWeights struct {
	cs []string
}

func (c zunionstoreWeights) Sum() zunionstoreAggregateSum {
	return zunionstoreAggregateSum{cs: append(c.cs, "SUM")}
}

func (c zunionstoreWeights) Min() zunionstoreAggregateMin {
	return zunionstoreAggregateMin{cs: append(c.cs, "MIN")}
}

func (c zunionstoreWeights) Max() zunionstoreAggregateMax {
	return zunionstoreAggregateMax{cs: append(c.cs, "MAX")}
}

func (c zunionstoreWeights) Weights(weights ...int64) zunionstoreWeights {
	for _, n := range weights {
		c.cs = append(c.cs, strconv.FormatInt(n, 10))
	}
	return zunionstoreWeights{cs: c.cs}
}

func (c zunionstoreWeights) Build() []string {
	return c.cs
}
