package main

import (
	"fmt"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/datastore/system"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/execution"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/logging"
	"github.com/couchbase/query/parser/n1ql"
	"github.com/couchbase/query/planner"
	"github.com/couchbase/query/timestamp"
	"github.com/couchbase/query/value"
	"github.com/gopherjs/gopherjs/js"
	"time"
	"encoding/json"
)

type JsKeyspace struct {
	*js.Object

	Name  string                                 `js:"name"`
	Count func() int                             `js:"count"`
	Keys  func() []string                        `js:"keys"`
	Fetch func(keys []string) []*JsAnnotatedPair `js:"fetch"`
}

type JsAnnotatedPair struct {
	*js.Object

	Id    string      `js:"id"`
	Value interface{} `js:"value"`
}

type JsOutput struct {
	*js.Object

	Item    func(item interface{}) `js:"item"`
	Done    func()                 `js:"done"`
	Fatal   func(err error)        `js:"fatal"`
	Error   func(err error)        `js:"error"`
	Warning func(wrn error)        `js:"warning"`
}

type JsDatastoreStub struct {
	namespace *JsNamespaceStub
}

func (s *JsDatastoreStub) Id() string {
	return "jsds"
}
func (s *JsDatastoreStub) URL() string {
	return "js:private"
}
func (s *JsDatastoreStub) NamespaceIds() ([]string, errors.Error) {
	return []string{"default"}, nil
}
func (s *JsDatastoreStub) NamespaceNames() ([]string, errors.Error) {
	return []string{"default"}, nil
}
func (s *JsDatastoreStub) NamespaceById(name string) (datastore.Namespace, errors.Error) {
	return s.NamespaceByName(name)
}
func (s *JsDatastoreStub) NamespaceByName(name string) (datastore.Namespace, errors.Error) {
	if name != "default" {
		return nil, errors.NewError(nil, "`"+name+"`")
	}
	return s.namespace, nil
}
func (s *JsDatastoreStub) Authorize(privs datastore.Privileges, creds datastore.Credentials) errors.Error {
	return nil
}
func (s *JsDatastoreStub) SetLogLevel(level logging.Level) {
}
func (s *JsDatastoreStub) Inferencer(name datastore.InferenceType) (datastore.Inferencer, errors.Error) {
	return nil, errors.NewOtherNotImplementedError(nil, "INFER")
}
func (s *JsDatastoreStub) Inferencers() ([]datastore.Inferencer, errors.Error) {
	return nil, errors.NewOtherNotImplementedError(nil, "INFER")
}

type JsNamespaceStub struct {
	datastore *JsDatastoreStub
	keyspaces []*JsKeyspaceStub
}

func (n *JsNamespaceStub) DatastoreId() string {
	return n.datastore.Id()
}
func (n *JsNamespaceStub) Id() string {
	return "default"
}
func (n *JsNamespaceStub) Name() string {
	return "default"
}
func (n *JsNamespaceStub) KeyspaceIds() ([]string, errors.Error) {
	return n.KeyspaceNames()
}
func (n *JsNamespaceStub) KeyspaceNames() ([]string, errors.Error) {
	var ksNames []string
	for _, keyspace := range n.keyspaces {
		ksNames = append(ksNames, keyspace.js.Name)
	}
	return ksNames, nil
}
func (n *JsNamespaceStub) KeyspaceById(name string) (datastore.Keyspace, errors.Error) {
	return n.KeyspaceByName(name)
}
func (n *JsNamespaceStub) KeyspaceByName(name string) (datastore.Keyspace, errors.Error) {
	var ks *JsKeyspaceStub
	for _, keyspace := range n.keyspaces {
		if keyspace.js.Name == name {
			ks = keyspace
			break
		}
	}
	if ks == nil {
		return nil, errors.NewOtherKeyspaceNotFoundError(nil, "`"+name+"`")
	}
	return ks, nil
}

type JsKeyspaceStub struct {
	js        *JsKeyspace
	namespace *JsNamespaceStub
	indexers  []*JsIndexerStub
}

func (k *JsKeyspaceStub) NamespaceId() string {
	return k.namespace.Id()
}
func (k *JsKeyspaceStub) Id() string {
	return k.Name()
}
func (k *JsKeyspaceStub) Name() string {
	return k.js.Name
}
func (k *JsKeyspaceStub) Count() (int64, errors.Error) {
	return 0, nil
}
func (k *JsKeyspaceStub) Indexer(name datastore.IndexType) (datastore.Indexer, errors.Error) {
	if name == datastore.DEFAULT {
		name = datastore.GSI
	}

	var q *JsIndexerStub
	for _, indexer := range k.indexers {
		if indexer.name == name {
			q = indexer
			break
		}
	}

	if q == nil {
		return nil, errors.NewOtherNotImplementedError(nil, "unknown indexer")
	}
	return q, nil
}
func (k *JsKeyspaceStub) Indexers() ([]datastore.Indexer, errors.Error) {
	var indexers []datastore.Indexer
	for _, indexer := range k.indexers {
		indexers = append(indexers, indexer)
	}
	return indexers, nil
}
func (k *JsKeyspaceStub) Fetch(keys []string) ([]value.AnnotatedPair, []errors.Error) {
	docs := k.js.Fetch(keys)

	var docsOut []value.AnnotatedPair
	for _, doc := range docs {
		docOut := value.NewAnnotatedValue(doc.Value)
		docOut.SetAttachment("meta", map[string]interface{}{"id": doc.Id})

		docsOut = append(docsOut, value.AnnotatedPair{
			Name:  doc.Id,
			Value: docOut,
		})
	}
	return docsOut, nil
}
func (k *JsKeyspaceStub) Insert(inserts []value.Pair) ([]value.Pair, errors.Error) {
	return nil, errors.NewOtherNotImplementedError(nil, "JsKeyspaceStub.Insert")
}
func (k *JsKeyspaceStub) Update(updates []value.Pair) ([]value.Pair, errors.Error) {
	return nil, errors.NewOtherNotImplementedError(nil, "JsKeyspaceStub.Update")
}
func (k *JsKeyspaceStub) Upsert(upserts []value.Pair) ([]value.Pair, errors.Error) {
	return nil, errors.NewOtherNotImplementedError(nil, "JsKeyspaceStub.Upsert")
}
func (k *JsKeyspaceStub) Delete(deletes []string) ([]string, errors.Error) {
	return nil, errors.NewOtherNotImplementedError(nil, "JsKeyspaceStub.Delete")
}
func (k *JsKeyspaceStub) Release() {
}

type JsIndexerStub struct {
	js         *JsKeyspace
	keyspace   *JsKeyspaceStub
	primaryIdx *JsPrimaryIndexStub
	name       datastore.IndexType
}

func (q *JsIndexerStub) KeyspaceId() string {
	return q.keyspace.Id()
}
func (q *JsIndexerStub) Name() datastore.IndexType {
	return q.name
}
func (q *JsIndexerStub) IndexIds() ([]string, errors.Error) {
	return q.IndexNames()
}
func (q *JsIndexerStub) IndexNames() ([]string, errors.Error) {
	var idxNames []string
	idxNames = append(idxNames, q.primaryIdx.Name())
	return idxNames, nil
}
func (q *JsIndexerStub) IndexById(id string) (datastore.Index, errors.Error) {
	return q.IndexByName(id)
}
func (q *JsIndexerStub) IndexByName(name string) (datastore.Index, errors.Error) {
	if name == q.primaryIdx.Name() {
		return q.primaryIdx, nil
	}
	return nil, errors.NewOtherIdxNotFoundError(nil, "`"+name+"`")
}
func (q *JsIndexerStub) PrimaryIndexes() ([]datastore.PrimaryIndex, errors.Error) {
	return []datastore.PrimaryIndex{q.primaryIdx}, nil
}
func (q *JsIndexerStub) Indexes() ([]datastore.Index, errors.Error) {
	return []datastore.Index{q.primaryIdx}, nil
}
func (q *JsIndexerStub) CreatePrimaryIndex(requestId, name string, with value.Value) (datastore.PrimaryIndex, errors.Error) {
	return nil, errors.NewOtherNotImplementedError(nil, "JsIndexerStub.CreatePrimaryIndex")
}
func (q *JsIndexerStub) CreateIndex(requestId, name string, seekKey, rangeKey expression.Expressions, where expression.Expression, with value.Value) (datastore.Index, errors.Error) {
	return nil, errors.NewOtherNotImplementedError(nil, "JsIndexerStub.CreateIndex")
}
func (q *JsIndexerStub) BuildIndexes(requestId string, name ...string) errors.Error {
	return errors.NewOtherNotImplementedError(nil, "JsIndexerStub.BuildIndexes")
}
func (q *JsIndexerStub) Refresh() errors.Error {
	return nil
}
func (q *JsIndexerStub) SetLogLevel(level logging.Level) {
}

type JsPrimaryIndexStub struct {
	js       *JsKeyspace
	indexer  *JsIndexerStub
	keyspace *JsKeyspaceStub
	name     string
}

func (m *JsPrimaryIndexStub) KeyspaceId() string {
	return m.keyspace.Id()
}
func (m *JsPrimaryIndexStub) Id() string {
	return m.Name()
}
func (m *JsPrimaryIndexStub) Name() string {
	return m.name
}
func (m *JsPrimaryIndexStub) Type() datastore.IndexType {
	return m.indexer.Name()
}
func (m *JsPrimaryIndexStub) SeekKey() expression.Expressions {
	return nil
}
func (m *JsPrimaryIndexStub) RangeKey() expression.Expressions {
	return nil
}
func (m *JsPrimaryIndexStub) Condition() expression.Expression {
	return nil
}
func (m *JsPrimaryIndexStub) IsPrimary() bool {
	return true
}
func (m *JsPrimaryIndexStub) State() (state datastore.IndexState, msg string, err errors.Error) {
	return datastore.ONLINE, "", nil
}
func (m *JsPrimaryIndexStub) Statistics(requestId string, span *datastore.Span) (datastore.Statistics, errors.Error) {
	return nil, nil
}
func (m *JsPrimaryIndexStub) Drop(requestId string) errors.Error {
	return errors.NewOtherIdxNoDrop(nil, "This primary index cannot be dropped for JS datastore.")
}
func (m *JsPrimaryIndexStub) Scan(requestId string, span *datastore.Span, distinct bool, limit int64, cons datastore.ScanConsistency, vector timestamp.Vector, conn *datastore.IndexConnection) {
	defer close(conn.EntryChannel())

	// For primary indexes, bounds must always be strings, so we
	// can just enforce that directly
	low, high := "", ""

	// Ensure that lower bound is a string, if any
	if len(span.Range.Low) > 0 {
		a := span.Range.Low[0].Actual()
		switch a := a.(type) {
		case string:
			low = a
		default:
			conn.Error(errors.NewOtherDatastoreError(nil, fmt.Sprintf("Invalid lower bound %v of type %T.", a, a)))
			return
		}
	}

	// Ensure that upper bound is a string, if any
	if len(span.Range.High) > 0 {
		a := span.Range.High[0].Actual()
		switch a := a.(type) {
		case string:
			high = a
		default:
			conn.Error(errors.NewOtherDatastoreError(nil, fmt.Sprintf("Invalid upper bound %v of type %T.", a, a)))
			return
		}
	}

	allKeys := m.js.Keys()
	allKeyCount := len(allKeys)

	reslimit := int(limit)
	if reslimit <= 0 {
		reslimit = allKeyCount
	}

	for i := 0; i < allKeyCount && i < reslimit; i++ {
		id := allKeys[i]

		if low != "" &&
			(id < low ||
				(id == low && (span.Range.Inclusion&datastore.LOW == 0))) {
			continue
		}

		low = ""

		if high != "" &&
			(id > high ||
				(id == high && (span.Range.Inclusion&datastore.HIGH == 0))) {
			break
		}

		entry := datastore.IndexEntry{PrimaryKey: id}
		conn.EntryChannel() <- &entry
	}
}
func (m *JsPrimaryIndexStub) ScanEntries(requestId string, limit int64, cons datastore.ScanConsistency, vector timestamp.Vector, conn *datastore.IndexConnection) {
	// ScanEntries for a primary index is the same as a normal indexes full scan
	m.Scan(requestId, &datastore.Span{}, false, limit, cons, vector, conn)
}

func NewJsDatastore(keyspaces []*JsKeyspace) (datastore.Datastore, errors.Error) {
	dss := &JsDatastoreStub{}

	nss := &JsNamespaceStub{}
	nss.datastore = dss
	dss.namespace = nss

	for _, keyspace := range keyspaces {
		kss := &JsKeyspaceStub{}
		kss.js = keyspace
		nss.keyspaces = append(nss.keyspaces, kss)

		gsi := &JsIndexerStub{}
		gsi.js = keyspace
		gsi.keyspace = kss
		gsi.name = datastore.GSI
		kss.indexers = append(kss.indexers, gsi)

		iss := &JsPrimaryIndexStub{}
		iss.js = keyspace
		iss.keyspace = kss
		iss.indexer = gsi
		iss.name = "#primary"
		gsi.primaryIdx = iss
	}

	return dss, nil
}

type JsOutputStub struct {
	js *JsOutput
}

func (o *JsOutputStub) Result(item value.Value) bool {
	// Items are of a funny variety
	bytes, _ := json.Marshal(item)
	var itemReal interface{}
	json.Unmarshal(bytes, &itemReal)
	o.js.Item(itemReal)
	return true
}
func (o *JsOutputStub) CloseResults() {
	o.js.Done()
}
func (o *JsOutputStub) Fatal(err errors.Error) {
	o.js.Fatal(err)
}
func (o *JsOutputStub) Error(err errors.Error) {
	o.js.Error(err)
}
func (o *JsOutputStub) Warning(wrn errors.Error) {
	o.js.Warning(wrn)
}
func (o *JsOutputStub) AddMutationCount(uint64) {
}
func (o *JsOutputStub) MutationCount() uint64 {
	return 0
}
func (o *JsOutputStub) SortCount() uint64 {
	return 0
}
func (o *JsOutputStub) SetSortCount(i uint64) {
}
func (o *JsOutputStub) AddPhaseOperator(p execution.Phases) {
}
func (o *JsOutputStub) AddPhaseCount(p execution.Phases, c uint64) {
}
func (o *JsOutputStub) FmtPhaseCounts() map[string]interface{} {
	return nil
}
func (o *JsOutputStub) FmtPhaseOperators() map[string]interface{} {
	return nil
}
func (o *JsOutputStub) AddPhaseTime(phase string, duration time.Duration) {
}
func (o *JsOutputStub) PhaseTimes() map[string]time.Duration {
	return nil
}
func (o *JsOutputStub) FmtPhaseTimes() map[string]interface{} {
	return nil
}

type JsVectorSourceStub struct {
}

func (v *JsVectorSourceStub) Type() int32 {
	return timestamp.NO_VECTORS
}
func (v *JsVectorSourceStub) ScanVector(namespace_id string, keyspace_name string) timestamp.Vector {
	return nil
}

const NAMESPACE = "default"

func Execute(keyspaces []*JsKeyspace, statement string, handler *JsOutput) error {
	stmt, err := n1ql.ParseStatement(statement)
	if err != nil {
		return err
	}

	dstore, _ := NewJsDatastore(keyspaces)
	sstore, _ := system.NewDatastore(dstore)

	op, err := planner.Build(stmt, dstore, sstore, NAMESPACE, false)
	if err != nil {
		return err
	}

	output := JsOutputStub{}
	output.js = handler

	ctx := execution.NewContext("FAKE-REQ-ID", dstore, sstore, NAMESPACE, false, 1, nil, nil, nil, datastore.UNBOUNDED, &JsVectorSourceStub{}, &output)

	eop, err := execution.Build(op, ctx)
	if err != nil {
		return err
	}

	eop.RunOnce(ctx, nil)
	return nil
}

func JsExecute(keyspaces []*JsKeyspace, statement string, handler *JsOutput) {
	go func() {
		err := Execute(keyspaces, statement, handler)
		if err != nil {
			handler.Fatal(err)
		}
	}()
}

func main() {
	exports := map[string]interface{}{
		"Execute": JsExecute,
	}

	js.Module.Set("exports", exports)
	js.Global.Set("n1qljs", exports)
}
