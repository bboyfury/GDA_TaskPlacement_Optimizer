
from thrift.server import TServer
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport
from thrift.transport import TSocket

# from ReduceTaskPlacementIface import *
from pulp import *
import collections
import threading
import json
import math
import time

sys.path.append("./gen-py")

# from dataplacement.ttypes import *


class ReduceTaskPlacement_handler:
    def __init__(self, placement):
        self.placement = placement


class Tetrium:
    msg = 0
    epgap = 0.05  # more precision less speed and vice versa.
    # using this knob we can maximize speed or minimize the cost.
    # when knob is 1, a job has maximum WAN budget
    # when knob is 0, WAN usage is minimized for each job.
    # only 0 or 1
    WAN_Knob = 1
    # based on paper, tred is constant but we dont know what is its default value in their evaluations.
    # tred is time of a reduce task, an average time for tasks.
    # however It can varies based on different factors such as memory or cpu ...
    # based on paper I assume that it takes around 8 secs for 400 reduce tasks so in each sec 50 task
    # 1.2 milliscond => estimate => note that it is important to know close estimation for get presice calculation.
    tred = 0.12

# get Fraction of task of each site=> formula is  IshufleX*(1-rx) / bUpx for transfer from
# and for transfer to is y!=xIshufly · rx/BdownX
# note that by divide by Bupx and Bdown X we get the duration.
# number of reduce task of site x is nred*rx  => number of reduce task * fraction of reduce tasks
# number of waves to complete tasks=> (nred*rx)/ Sx  sx= number of compute slots.
# computation time of site  x is tred*((nred*rx)/Sx)
# based on Tetrium's paper the tred is constant idk why??????????? ask DR. Oh.
# tred is computation time of a reduce task.

    # first we need to minimize computation time

    # get Tred and Tshufl
    # first we need to minimize Network  time
    # get Tshufl
    # note that we want to minimize sume of network and computation time.
    # *********HELPERS*******

    def setGlobalVariablesFromInput(self, inputs):
        self.cost = inputs["cost_info"]
        self.goals = inputs["goals"]
        self.monitoring_info = inputs["monitoring_info"]
        self.data_size = inputs["data_size"]
        self.dc_list = self.monitoring_info.keys()

        # get number of reducer tasks.
        if len(self.dc_list) == 1:
            self.number_of_reducer = 200
        else:
            self.number_of_reducer = max(
                len(self.data_size[dc]) for dc in self.dc_list if dc in self.data_size)

        self.siteDataSize = {}
        self.availableCoresOfDC = {}
        self.availableCpuOfDC = {}
        self.taskDataSize = {}

        for dc in self.dc_list:

            self.availableCoresOfDC[dc] = round(self.monitoring_info[dc]['computing_resources']['node_cores']
                                                * (1 - self.monitoring_info[dc]['computing_resources']['cpu_us']), 4)
            self.availableCpuOfDC[dc] = str(
                (1 - self.monitoring_info[dc]['computing_resources']['cpu_us']) * 100)
            self.siteDataSize[dc] = sum(self.data_size[dc].values())

            for _from in self.dc_list:
                for _to in self.dc_list:
                    if _from == _to:
                        # local
                        self.monitoring_info[_from]['network_bandwidth'][_to] = 1e12
                    else:
                        if _to not in self.monitoring_info[_from]['network_bandwidth']:
                            # assume 5MB
                            self.monitoring_info[_from]['network_bandwidth'][_to] = 5e6
                        else:
                            value = self.monitoring_info[_from]['network_bandwidth'][_to]
                            if isinstance(value, list) == True:
                                # uplink is sum of BW of all outward connections / number of them
                                self.monitoring_info[_from]['network_bandwidth'][_to] = sum(
                                    value) / float(len(value))
                            else:
                                self.monitoring_info[_from]['network_bandwidth'][_to] = value / 1.0

            # get data size
        self.siteDataSize[dc] = sum(self.data_size[dc].values())
        self.TotalDataSize = sum(self.siteDataSize.values())
        # GETS  the sum of each tasks datasize, for example it can be sum of task 1, and task 1 is in dc 1 and 2, so sum of these two is the result.

        for _reduce_id in range(0, self.number_of_reducer):
            self.taskDataSize[str(_reduce_id)] = sum(
                self.data_size[_from][str(_reduce_id)] for _from in self.dc_list)
        for dc in self.dc_list:
            self.printGeneralInfoOfDC(dc)

            #########
        self.PrintGeneralInfo()

    def PrintGeneralInfo(self):
        print('-- Current computating resource infomation --')
        print('Total Data Size: ' + str(self.TotalDataSize))
        print('Total number of reducer: ' + str(self.number_of_reducer))

    def printResult(self, taskPlacement, text):
        print("-------------------------------------------------")
        print("-- Found optimized task placement")
        self.printCalculatedTaskPlacement(taskPlacement)
        print("-------------------------------------------------")
        print(text)
        # todo
        cost = self.getCost(taskPlacement)
        networkDuration, computeDuration = self.getDuration(taskPlacement)

        print(
            " - cost: "
            + str(cost)
            + " duration: "
            + str(networkDuration+computeDuration)
            + " network: ("
            + str(networkDuration)
            + " + compute:  "
            + str(computeDuration)
            + ")"
        )

    # *********End of HELPERS*******

    def getCostFromGBBased(self, GB):
        _cost = GB / 1e9
        return _cost

    def getTasksSize(self, taskPlacement):

        taskSize = {}

        for reduce_task_id in taskPlacement:
            _to = taskPlacement[reduce_task_id]

            for _from in self.dc_list:

                if _to != _from:
                    taskSize[reduce_task_id] = self.data_size[_from][str(
                        reduce_task_id)]

        return taskSize

    def printGeneralInfoOfDC(self, dc):
        print(dc + '-> Available CPU: ' +
              self.availableCpuOfDC[dc] + '-----' + 'available Cores: ' + str(self.availableCoresOfDC[dc]))

    def printCalculatedTaskPlacement(self, taskPlacement):
        inv_map = {}
        for k, v in taskPlacement.items():
            inv_map[v] = inv_map.get(v, [])
            inv_map[v].append(int(k))

        for hostname in inv_map:
            print(hostname + " -> " + str(inv_map[hostname]))

  # ****************LP****************
    def getDurationUsingFraction(self, fraction):

        Tshufl = 0.0
        MaxDuration = 0.0
        MaxNetworkTransferDuration = 0.0
        DownNetworkTransferDuration = 0
        MaxComputationDuration = 0.0

        for _from in self.dc_list:
            UpNetworkTransferDuration = 0
            DownNetworkTransferDuration = 0
            ComputationDuration = 0
            aggregated = 0
            networkBottleNeck = 0

            for _to in self.dc_list:
                UpNetworkTransferDuration = (
                    ((1 - fraction[_to])
                     * self.siteDataSize[_from])
                    / self.monitoring_info[_from]["network_bandwidth"][_to]
                )
                if _from != _to:
                    DownNetworkTransferDuration = (
                        ((fraction[_to])
                         * self.siteDataSize[_from])
                        / self.monitoring_info[_to]["network_bandwidth"][_from]
                    )

                Tshufl += UpNetworkTransferDuration
                networkBottleNeck = min(
                    UpNetworkTransferDuration, DownNetworkTransferDuration)

                # The number of reduce-tasks at site x is nred * rx
                aggregated = self.number_of_reducer * fraction[_to]
                coresNumber = self.availableCpuOfDC[_to]
                waves = aggregated / float(coresNumber)
                ComputationDuration = self.tred * waves
# note that maximum amount of both network and compute is our measure
                if MaxDuration < networkBottleNeck + ComputationDuration:
                    MaxNetworkTransferDuration = networkBottleNeck
                    MaxComputationDuration = ComputationDuration
                    MaxDuration = networkBottleNeck + ComputationDuration

        return MaxNetworkTransferDuration, MaxComputationDuration, Tshufl

    def LpCalculateForShuffle(self):
        generalTimer = time.time()

        # first we need to find minimum of network and computation duration time
        Fraction = self.GetRx()
        minNetworkDuration, minComputationDuration, Tshufl = self.getDurationUsingFraction(
            Fraction)
        # final Output for min computation and network duration
        #
        totalDuration = sum([minNetworkDuration, minComputationDuration])
        print('**********************')
        print('total status of current data before solving by LP')
        print('Total Duration: ' + str(totalDuration))
        print('Total shuffle time: ' + str(Tshufl))
        # now we have computation duration and network duration
        # we should find optimal task placement.
        maxDurationVariable = self.getMaxDurationVariable()
        reduceTaskPlacementVariable = self.getReduceTaskPlacementVariable()
        cost = self.getTotalCostWithFraction(self.GetRx())
        
        # minimize query duration
        model = LpProblem("Min query duration", LpMinimize)
        self.setConstraints(model, maxDurationVariable,
                            reduceTaskPlacementVariable)
        self.setObjective(model, maxDurationVariable,
                          reduceTaskPlacementVariable)
        solver = GUROBI(msg=self.msg, epgap=self.epgap)
        status = model.solve(solver)
        model.writeLP("Tetrium.lp")
# todo
        end = time.time()

        if status > 0:
            reduce_taskPlacement = self.calculateTaskPLacement(
                reduceTaskPlacementVariable
            )

            # print json.dumps(reduce_taskPlacement, indent=4, sort_keys=True)
            self.printResult(
                reduce_taskPlacement,
                "$$$$$$$$$$$$$$$$$$$$$$$$"
                + ""
                + "total Cost: "
                + str(cost))
            print(
                "Take time: "
                + str(end - generalTimer)

            )
            #todo
            estimatedDuration = self.estimateDuration(
                reduce_taskPlacement)

            return json.dumps(reduce_taskPlacement)
        else:
            print("failed to determine a placement")
            return False

    def calculateTaskPLacement(self, reduceTasKPlacementVariable):
        reduceTasKPlacement = {}
        for taskId in range(0, self.number_of_reducer):
            for _to in self.dc_list:
                if (
                    reduceTasKPlacementVariable[(str(taskId), _to)].varValue
                    > 0.000001
                ):
                    reduceTasKPlacement[taskId] = _to
                    # todo
        taskSizes = self.getTasksSize(reduceTasKPlacement)
        sortedTaskSizes = {k: v for k, v in sorted(
            taskSizes.items(), key=lambda item: item[1], reverse=True)}
        # sort tasks (biggest task comes first)

        sortedReduceTasKPlacement = {k: reduceTasKPlacement[k] for k in sorted(
            reduceTasKPlacement, key=lambda x: sortedTaskSizes[x], reverse=True)}

        return sortedReduceTasKPlacement

    def getExpectedDuration(self, task_id, _to):
        max_expected_networkTransferDuration = 0
        expected_networkTransferDuration = 0
        for _from in self.dc_list:
            self.data_size[_from][str(task_id)]
            expected_networkTransferDuration = (
                self.data_size[_from][str(task_id)]
                / self.monitoring_info[_from]["network_bandwidth"][_to]
            )
            if max_expected_networkTransferDuration < expected_networkTransferDuration:
                max_expected_networkTransferDuration = expected_networkTransferDuration
        return max_expected_networkTransferDuration

    def estimateDuration(self, taskPlacement):
        estimatedDuration = {}

        for task_index in taskPlacement:
            hostname = taskPlacement[task_index]
            expected_duration = self.getExpectedDuration(task_index, hostname)

            estimatedDuration[task_index] = {}
            estimatedDuration[task_index]["target_host"] = hostname
            estimatedDuration[task_index]["expected_duration"] = expected_duration
        return estimatedDuration

    def setConstraints(self, model, maxDurationVariable, reduceTaskPlacementVariable):

        # xrx==1
        for reduce_task_id in range(0, self.number_of_reducer):
            # for each DC sum of fractions should equal to 1
            model += (
                lpSum(
                    reduceTaskPlacementVariable[(str(reduce_task_id), _to)]
                    for _to in self.dc_list
                )
                == 1.0,
                "For each partition (reducer id: " + str(reduce_task_id) + ")",
            )

        IshufleUp = {}
        IshufleDown = {}
        aggregated_size = {}
        taskCount = {}
        rx = {}
        # X ixShufle // find data need to be go for site x

        for _from in self.dc_list:
            IshufleUp[_from] = {}
            IshufleDown[_from] = {}
            for _to in self.dc_list:

                IshufleUp[_from][_to] = lpSum(
                    (reduceTaskPlacementVariable[(str(reduce_task_id), _to)])
                    * self.data_size[_from][str(reduce_task_id)]
                    for reduce_task_id in range(0, self.number_of_reducer)
                )
                if _to != _from:
                    IshufleDown[_from][_to] = lpSum(
                        reduceTaskPlacementVariable[(str(reduce_task_id), _to)]
                        * self.data_size[_from][str(reduce_task_id)]
                        for reduce_task_id in range(0, self.number_of_reducer)
                    )
        for _to in self.dc_list:
            aggregated_size[_to] = lpSum(
                reduceTaskPlacementVariable[(str(reduce_task_id), _to)]
                * self.data_size[_from][str(reduce_task_id)]

                for reduce_task_id in range(0, self.number_of_reducer)
                for _from in self.dc_list
            )

        # find Tshufl// network transfer duration of reduce-stage
        # formula is I shufle*1-rx / bupx
        # formula is I shufle*rx / bdown
        for _from in self.dc_list:
            Tshufle = 0.0
            Tred = 0.0
            for _to in self.dc_list:
                if self.WAN_Knob == 0:
                    Tshufle = lpSum(
                        reduceTaskPlacementVariable[(str(reduce_task_id), _to)]
                        * self.data_size[_from][str(reduce_task_id)]
                        for reduce_task_id in range(0, self.number_of_reducer)
                        for _from in self.dc_list
                        for _to in self.dc_list
                    )
                else:
                    Tshufle = (
                        (1 - IshufleUp[_from][_to])
                        / self.monitoring_info[_from]["network_bandwidth"][_to]
                    )
                    if _to != _from:
                        Tshufle = (
                            IshufleDown[_from][_to]
                            / self.monitoring_info[_to]["network_bandwidth"][_from]
                        )


                    Tred = aggregated_size[_to] / (
                        self.availableCoresOfDC[_to]

                    )

                model += maxDurationVariable >= Tshufle + Tred

    def setObjective(self, model, maxDurationVariable, taskPlacement_variable):
        model += maxDurationVariable, "minDurationConstraint"

    def getMaxDurationVariable(self):
        maxDurationvariable = LpVariable("maxDuration", 0)
        return maxDurationvariable

    def getReduceTaskPlacementVariable(self):
        reduceTaskPlacementVariable = LpVariable.dicts(
            "taskPlacement",
            [
                (str(reduce_task_id), _to)
                for reduce_task_id in range(0, self.number_of_reducer)
                for _to in self.dc_list
            ],
            0,
            1,
            LpBinary,
        )
        return reduceTaskPlacementVariable

    def GetRx(self):
        model = LpProblem("min query duration", LpMinimize)
        fractionVariable = LpVariable.dicts(
            "fraction", [_dc for _dc in self.dc_list], 0, 1, cat="Continuous"
        )
        maxDurationVariable = LpVariable("max duration", 0)
        # constraints
        model += lpSum(fractionVariable[_dc] for _dc in self.dc_list) == 1
        for _from in self.dc_list:
            for _to in self.dc_list:
                UpNetworkTransferDuration = (
                    ((1 - fractionVariable[_to])
                     * self.siteDataSize[_from])
                    / self.monitoring_info[_from]["network_bandwidth"][_to]
                )


                aggregated = self.number_of_reducer * fractionVariable[_to]
                coresNumber = self.availableCpuOfDC[_to]
                waves = aggregated / float(coresNumber)
                ComputationDuration = self.tred * waves
                model += maxDurationVariable >= UpNetworkTransferDuration + ComputationDuration
        model += maxDurationVariable, "minDuration"
        model.writeLP("minRx.lp")

        solver = GUROBI(msg=self.msg, epgap=self.epgap)
        status = model.solve(solver)
        if status > 0:
            return self.getFractionValueByVariable(fractionVariable)
        else:
            return False

    def getFractionValueByVariable(self, fractionVariable):
        rx = {}
        for dc in self.dc_list:
            rx[dc] = float(format(fractionVariable[dc].varValue, ".16f"))

        return rx

    def getTotalCostWithFraction(self, fraction):
        cost = 0
        for _from in self.dc_list:
            for _to in self.dc_list:
                # only uplink
                cost += (
                    self.siteDataSize[_from]
                    * fraction[_to]
                    * self.getCostFromGBBased(self.cost[_from]["network_cost"][_to])
                )
        return cost

    def getDuration(self, task_placement):
        Tshufl = 0.0
        MaxDuration = 0.0
        MaxNetworkTransferDuration = 0.0
        DownNetworkTransferDuration = 0
        MaxComputationDuration = 0.0

        from_to_bytes = {}
        aggregated_size = {}
        task_count = {}

        for dc in self.dc_list:
            from_to_bytes[dc] = {}
            aggregated_size[dc] = 0
            task_count[dc] = 0

            for _to in self.dc_list:
                from_to_bytes[dc][_to] = 0

        for reduce_task_id in task_placement:
            _to = task_placement[reduce_task_id]

            task_count[_to] += 1

            for _from in self.dc_list:
                from_to_bytes[_from][_to] += self.data_size[_from][str(
                    reduce_task_id)]
                aggregated_size[_to] += self.data_size[_from][str(
                    reduce_task_id)]

        for _from in self.dc_list:
            UpNetworkTransferDuration = 0
            DownNetworkTransferDuration = 0
            ComputationDuration = 0
            aggregated = 0
            networkBottleNeck = 0

            for _to in self.dc_list:
                UpNetworkTransferDuration = (
                    ((1 - (self.number_of_reducer/task_count[dc]))
                     * from_to_bytes[_from][_to])
                    / self.monitoring_info[_from]["network_bandwidth"][_to]
                )
                if _from != _to:
                    DownNetworkTransferDuration = (
                        ((1 - (self.number_of_reducer/task_count[_from]))
                         * from_to_bytes[_from][_to])
                        / self.monitoring_info[_to]["network_bandwidth"][_from]
                    )

                Tshufl += UpNetworkTransferDuration
                networkBottleNeck = min(
                    UpNetworkTransferDuration, DownNetworkTransferDuration)
                if task_count[_to] > 0:

                    # The number of reduce-tasks at site x is nred * rx
                    aggregated = task_count[_to] * \
                        (self.number_of_reducer/task_count[_to])
                    coresNumber = self.availableCpuOfDC[_to]
                    waves = aggregated / float(coresNumber)
                    ComputationDuration = self.tred * waves

                else:
                    ComputationDuration = 0.0

                if MaxDuration < networkBottleNeck + ComputationDuration:
                    MaxNetworkTransferDuration = networkBottleNeck
                    MaxComputationDuration = ComputationDuration
                    MaxDuration = networkBottleNeck + ComputationDuration

        return MaxNetworkTransferDuration, MaxComputationDuration

    def getCost(self, taskPplacement):
        totalCost = 0

        for taskId in taskPplacement:
            _to = taskPplacement[taskId]
            # only upload
            for _from in self.dc_list:
                totalCost += self.data_size[_from][
                    str(taskId)
                ] * self.getCostFromGBBased(self.cost[_from]["network_cost"][_to])

        return totalCost

   # ****************end of LP****************

    def run_server(self, server_port):
        # set handler to our implementation
        handler = ReduceTaskPlacement_handler(self)

        processor = "ReduceTaskPlacementIface.Processor(handler)"
        transport = TSocket.TServerSocket(port=server_port)
        tfactory = TTransport.TFramedTransportFactory()
        pfactory = TBinaryProtocol.TBinaryProtocolFactory()

        # set server
        server = TServer.TThreadedServer(
            processor, transport, tfactory, pfactory, daemon=True
        )

        print(
            "[Reduce Task Placement] Starting applications server port:"
            + str(server_port)
        )
        server.serve()


# *********Formuls*******

# *********End of Formuls*******

# ********Main Modules***********

    def RunOptimizer(self, input):
        timer = time.time()

        self.setGlobalVariablesFromInput(input)
        reduce = self.LpCalculateForShuffle()
        end = time.time()
        print("Min Latency - Take time: " + str(end - timer) + " ms")
        return json.dumps(reduce)


# *****************************


# checks if the Python script is being run as the main module
if __name__ == "__main__":
    dataplacement_server = Tetrium()

    if len(sys.argv) == 1:
        dataplacement_server.run_server(55511)
    else:
        inputs = {}

        with open(sys.argv[1]) as data_file:
            inputs = json.load(data_file)
            reduce_taskPlacement = dataplacement_server.RunOptimizer(inputs)

            if reduce_taskPlacement == False:
                print(
                    "!!!!! No possible data placement to achieve the goal in current setting"
                )
