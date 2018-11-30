from py.log import Producer
from _pytest.runner import CollectReport

from xdist.workermanage import parse_spec_config
from xdist.report import report_collection_diff


class TemplateScheduling:
    """Implement template scheduling across nodes.

    This distributes the tests collected across all nodes so each test
    is run just once. All nodes collect and submit the test suite and
    when all collections are received it is verified they are
    identical collections. Then the collection gets divided by templates
    specified in dist-templ option and forms pending async groups. Tests
    which was collected but didn't match any template, get's copied to pending
    sync list and will be executed after each async group finish.
    Whenever a node finishes an item, it calls ``.mark_test_complete()``
    which will trigger the scheduler to assign more async groups if the number
    of pending tests for the node falls below a low-watermark. When all async
    groups get's finished, shutdown signal send to all nodes except first one
    that catches up pending sync list

    When created, ``numnodes`` defines how many nodes are expected to
    submit a collection. This is used to know when all nodes have
    finished collection or how large the chunks need to be created.

    Attributes:

    :config: Config object, used for handling hooks.

    :numnodes: The expected number of nodes taking part.  The actual
       number of nodes will vary during the scheduler's lifetime as
       nodes are added by the DSession as they are brought up and
       removed either because of a dead node or normal shutdown.  This
       number is primarily used to know when the initial collection is
       completed.

    :dist_templates: List of templates used to divide array of collected items
       into groups of pending tests that need to be run asynchronous

    :node_collection: Map of nodes and their test collection.  All
       collections should always be identical.

    :node_pending: Map of nodes and the indices of their pending
       tests.  The indices are an index into ``.pending_async``
       and ``.pending_sync`` (which is identical to their own collection
       stored in ``.node_collection``).

    :pending_async: List of groups of indices of pending tests that needs
       to be run asynchronous. These are tests which have not yet been
       allocated for a node to process.

    :pending_sync: List of indices of pending tests that needs to be run
       after completion of all asynchronous tests and shutdown of all nodes
       except first one.

    :collection: The one collection once it is validated to be
       identical between all the nodes.  It is initialised to None
       until ``.schedule()`` is called.

    :log: A py.log.Producer instance.
    """

    def __init__(self, config, log=None):
        self.config = config
        self.numnodes = len(parse_spec_config(config))
        self.dist_templates = self.config.getini("dist-templ")
        self.node_collection = {}
        self.node_pending = {}
        self.pending_async = []
        self.pending_sync = []
        self.collection = None
        if log is None:
            self.log = Producer("templsched")
        else:
            self.log = log.templsched

    @property
    def nodes(self):
        """A list of all nodes in the scheduler."""
        return list(self.node_pending.keys())

    @property
    def collection_is_completed(self):
        """Boolean indication initial test collection is complete.

        This is a boolean indicating all initial participating nodes
        have finished collection.  The required number of initial
        nodes is defined by ``.numnodes``.
        """
        return len(self.node_collection) >= self.numnodes

    @property
    def tests_finished(self):
        """Return True if all tests have been executed by the nodes."""
        if not self.collection_is_completed:
            return False
        if self.pending_async or self.pending_sync:
            return False
        for pending in self.node_pending.values():
            if len(pending) >= 2:
                return False
        return True

    @property
    def has_pending(self):
        """Return True if there are pending test items

        This indicates that collection has finished and nodes are
        still processing test items, so this can be thought of as
        "the scheduler is active".
        """
        if self.pending_async or self.pending_sync:
            return True
        for pending in self.node_pending.values():
            if pending:
                return True
        return False

    def add_node(self, node):
        """Add a new node to the scheduler.

        From now on the node will be allocated chunks of tests to
        execute.

        Called by the ``DSession.worker_workerready`` hook when it
        successfully bootstraps a new node.
        """
        assert node not in self.node_pending
        self.node_pending[node] = []

    def add_node_collection(self, node, collection):
        """Add the collected test items from a node

        The collection is stored in the ``.node_collection`` map.
        Called by the ``DSession.worker_collectionfinish`` hook.
        """
        assert node in self.node_pending
        # A new node has been added later, perhaps an original one died.
        if self.collection_is_completed:

            # Assert that .schedule() should have been called by now
            assert self.collection

            # Check that the new collection matches the official collection
            if collection != self.collection:
                other_node = next(iter(self.node_collection.keys()))

                msg = report_collection_diff(
                    self.collection,
                    collection,
                    other_node.gateway.id,
                    node.gateway.id
                )
                self.log(msg)
                return
        self.node_collection[node] = list(collection)

    def mark_test_complete(self, node, item_index, duration=0):
        """Mark test item as completed by node

        The duration it took to execute the item is used as a hint to
        the scheduler.

        This is called by the ``DSession.worker_testreport`` hook.
        """
        self.node_pending[node].remove(item_index)
        self.check_schedule(node)

    def remove_node(self, node):
        """Remove a node from the scheduler

        This should be called either when the node crashed or at
        shutdown time.  In the former case any pending items assigned
        to the node will be re-scheduled.  Called by the
        ``DSession.worker_workerfinished`` and
        ``DSession.worker_errordown`` hooks.

        Return the item which was being executing while the node
        crashed or None if the node has no more pending items.

        """
        pending = self.node_pending.pop(node)

        for node in self.node_pending:
            self.check_schedule(node)
        if not pending:
            return

        # The node crashed, reassign pending items
        rerun_count = self.config.cache.get("xdist/rerun", 0)
        pending_item = pending[0] if rerun_count > 0 else pending.pop(0)
        crashitem = self.collection[pending_item]
        self.pending_sync.extend(pending)
        for node in self.node_pending:
            self.check_schedule(node)
        return crashitem

    def check_schedule(self, node):
        """Maybe schedule new items on the node

        If there are any globally pending nodes left then this will
        check if the given node should be given any more tests.
        """
        # If there is only one node left and async groups list of empty,
        # take care about sync items list
        if len(self.nodes) == 1 and not self.pending_async \
                and self.pending_sync and not node.shutting_down:
            items_to_proceed = self.pending_sync
            self.pending_sync = []
            self.node_pending[self.nodes[0]].extend(items_to_proceed)
            self.nodes[0].send_runtest_some(items_to_proceed)
            self.nodes[0].shutdown()

        if node.shutting_down:
            return

        # If node is about to finish current pool (less than 2 items left).
        # Pick next group if there is still some groups in async list.
        if len(self.node_pending[node]) < 2 and self.pending_async:
            items_to_proceed = self.pending_async.pop(0)
            self.node_pending[node].extend(items_to_proceed)
            node.send_runtest_some(items_to_proceed)

        self._shutdown_unnecessary_nodes()

    def schedule(self):
        """Initiate distribution of the test collection

        Initiate scheduling of the items across the nodes.  If this
        gets called again later it behaves the same as calling
        ``.check_schedule()`` on all nodes so that newly added nodes
        will start to be used.

        This is called by the ``DSession.worker_collectionfinish`` hook
        if ``.collection_is_completed`` is True.
        """
        assert self.collection_is_completed

        # Initial distribution already happened, reschedule on all nodes
        if self.collection is not None:
            for node in self.nodes:
                self.check_schedule(node)
            return

        # XXX allow nodes to have different collections
        if not self._check_nodes_have_same_collection():
            self.log("**Different tests collected, aborting run**")
            return

        # Collections are identical, create the index of pending items.
        self.collection = list(self.node_collection.values())[0]
        if not self.collection:
            return

        pending = list(range(len(self.collection)))

        # Iterate through all templates from config
        for template in self.dist_templates:
            # If template starts with ! add item to sync list instead of async
            to_sync_list = template.startswith('!')
            if to_sync_list:
                template = template[1:]

            filters = template.split(',')

            # Collect all tests names matching any of the filters
            items = [i for i in self.collection
                     if any(f in i for f in filters)]

            # Get indices of items that matches filters
            item_indices = [self.collection.index(i) for i in items if
                            self.collection.index(i) in pending]
            if item_indices:
                self.pending_sync.extend(item_indices) if to_sync_list \
                    else self.pending_async.append(item_indices)
                pending = [p for p in pending if p not in item_indices]

        self.pending_sync.extend(pending)

        # Remove extra nodes
        if self.pending_async:
            extra_nodes = len(self.nodes) - len(self.pending_async)
            for _ in range(extra_nodes):
                self.nodes[-1].shutdown()
                self.node_pending.pop(self.nodes[-1])

        self._shutdown_unnecessary_nodes()

        for node in self.nodes:
            self.check_schedule(node)

    def _shutdown_unnecessary_nodes(self):
        """Send shutdown signal.

        Sends shutdown to all nodes except first one in case if all
        async items was distributed
        """
        # All async items distributed, shutdown all nodes starting from 2
        if not self.pending_async:
            for node in self.nodes[1:]:
                if not node.shutting_down:
                    node.shutdown()

    def _check_nodes_have_same_collection(self):
        """Return True if all nodes have collected the same items.

        If collections differ, this method returns False while logging
        the collection differences and posting collection errors to
        pytest_collectreport hook.
        """
        node_collection_items = list(self.node_collection.items())
        first_node, col = node_collection_items[0]
        same_collection = True

        for node, collection in node_collection_items[1:]:
            msg = report_collection_diff(
                col,
                collection,
                first_node.gateway.id,
                node.gateway.id,
            )
            if not msg:
                continue

            same_collection = False
            self.log(msg)

            if self.config is None:
                continue

            rep = CollectReport(
                node.gateway.id,
                'failed',
                longrepr=msg,
                result=[]
            )
            self.config.hook.pytest_collectreport(report=rep)

        return same_collection
