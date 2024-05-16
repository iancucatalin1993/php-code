<?php

namespace App\Services;

use App\Repositories\WorkforceProductivityRepository;
use Illuminate\Support\Collection;

define('HOUR_MINUTES', 60);

class WorkforceProductivityService
{
    private readonly WorkforceProductivityRepository $wpRepository;
    private readonly object $filters;
    private readonly int $defaultCurrency;

    private readonly Collection $billedFluxes;
    private readonly Collection $filteredWorklogs;
    private readonly Collection $allProjectsWorklogs;
    private readonly array $allInvoiceRowValues;
    private readonly array $usersCostsByMonth;

    //===============================================================================================================================
    // Constructor
    //===============================================================================================================================

    public function __construct(WorkforceProductivityRepository $wpRepository, object $filters, int $defaultCurrency = EUR)
    {
        $this->wpRepository = $wpRepository;
        $this->filters = $filters;
        $this->defaultCurrency = $defaultCurrency;

        $this->billedFluxes = $this->wpRepository->getBilledFluxes($this->filters);
        //get the invoices value row by row (to separate the values per project)
        $this->allInvoiceRowValues = $this->wpRepository->getAllInvoiceRowValues($this->filters);
        $fluxIds = $this->billedFluxes->keys()->all();

        //Step 2: get logs by flux ID, which will be grouped by each calculated category
        $this->filteredWorklogs = $this->wpRepository->getBilledLogs($fluxIds, $this->filters->projectsList);
        $this->allProjectsWorklogs = $this->wpRepository->getBilledLogs($fluxIds, null);
        $this->usersCostsByMonth = $this->getUsersCostsGroupedByMonth();

        //take into account the included value of subscription type project logs,
        //and eliminate that value from the user/activities income calculation
        $this->getSubscriptionsLogsValue($this->filteredWorklogs);
    }

    //===============================================================================================================================
    // Public
    //===============================================================================================================================

    public function getTotalIncomeAndCostsByMonth(): array
    {
        $incomePerMonth = [];
        $costsPerMonth = [];
        //get the total billed value by using the billing fluxes billed value
        foreach ($this->allInvoiceRowValues as $invoiceRow) {
            $incomePerMonth[$invoiceRow->invoice_month] ??= 0;
            $incomePerMonth[$invoiceRow->invoice_month] += $invoiceRow->billed_value;
        }
        foreach ($this->usersCostsByMonth as $month => $monthCosts) {
            $costsPerMonth[$month] = array_sum($monthCosts);
        }
        return [$incomePerMonth, $costsPerMonth];
    }

    /**  possible categories: users, activities */
    public function getMonthlyIncomeByFluxLogs(?Collection $worklogs, string $categoryKey, array $filteredCategoryIds,
                                               ?array &$finalIdsList = null): array
    {
        $billedFluxes = $this->billedFluxes;
        $worklogs ??= $this->filteredWorklogs;

        //group by flux ID and then user/activity ID
        $logsByFluxAndCategory = $worklogs->groupBy(['flux_id', $categoryKey]);
        $categoryIncomeByMonth = [];
        //get user logged valued relative to each billing flux
        $fluxLoggedValuesPerCategory = [];
        foreach ($logsByFluxAndCategory as $fluxId => $fluxLogs) {
            //ignore the fixed sum projects, we use the separate function "getUsersFixedSumProjectsValue" for those
            if ($billedFluxes[$fluxId]->payment_structure == PROJECT_PAYMENT_FIXED_SUM) continue;

            $fluxLoggedValuesPerCategory[$fluxId] ??= [];
            $fluxTotalLoggedValue = 0;
            $fluxMonth = $billedFluxes[$fluxId]->invoice_month;
            $categoryIncomeByMonth[$fluxMonth] ??= [];
            foreach ($fluxLogs as $categoryId => $categoryFluxLogs) {
                //sum by logs value key on user logs collection
                $categoryFluxLoggedValue = $categoryFluxLogs->sum('value');
                $fluxLoggedValuesPerCategory[$fluxId][$categoryId] = (object) [
                    'logged_value' => $categoryFluxLoggedValue
                ];
                $fluxTotalLoggedValue += $categoryFluxLoggedValue;
            }
            if ($billedFluxes[$fluxId]->payment_structure == PROJECT_PAYMENT_MONTHLY &&
                $fluxTotalLoggedValue < $billedFluxes[$fluxId]->billed_value
            ) {
                $fluxTotalLoggedValue = $billedFluxes[$fluxId]->billed_value;
            }
            //get user value percentages relative to the total flux value
            foreach ($fluxLoggedValuesPerCategory[$fluxId] as $categoryId => $categoryFluxValue) {
                //if the category has no ID, assign -1 as ID
                if ($categoryId === '') $categoryId = -1;
                //do not include unfiltered IDs
                if (!in_array($categoryId, $filteredCategoryIds)) continue;
                if (!is_null($finalIdsList) && !in_array($categoryId, $finalIdsList)) $finalIdsList[] = $categoryId;

                $categoryFluxValue->percentage_logged = 0;
                if ($fluxTotalLoggedValue) {
                    $categoryFluxValue->percentage_logged = $categoryFluxValue->logged_value / $fluxTotalLoggedValue;
                }
                $categoryFluxValue->billed_value = $billedFluxes[$fluxId]->billed_value * $categoryFluxValue->percentage_logged;
                $categoryIncomeByMonth[$fluxMonth][$categoryId] ??= 0;
                $categoryIncomeByMonth[$fluxMonth][$categoryId] += $categoryFluxValue->billed_value;
            }
        }

        return $categoryIncomeByMonth;
    }

    /**  possible categories: projects, owners */
    public function getIncomeByBilledValue(string $categoryKey, array $filteredIds, array &$finalIdsList): array
    {
        //get billed value for each category, using the billing fluxes total billed value
        $categoryIncomePerMonth = [];
        foreach ($this->allInvoiceRowValues as $invoiceRow) {
            if (!isset($invoiceRow->$categoryKey)) continue;
            $categoryId = $invoiceRow->$categoryKey;
            if (!in_array($categoryId, $filteredIds)) continue;

            if (!in_array($categoryId, $finalIdsList)) $finalIdsList[] = $categoryId;
            $categoryIncomePerMonth[$invoiceRow->invoice_month][$categoryId] ??= 0;
            $categoryIncomePerMonth[$invoiceRow->invoice_month][$categoryId] += $invoiceRow->billed_value;
        }

        return $categoryIncomePerMonth;
    }

    /** possible categories: projects, owners, activities */
    public function getCategoryCostsByUserLogs(string $categoryKey): array
    {
        //get total user costs for each user and then for each category, per month
        $userValuesPerCategory = [];
        foreach ($this->allProjectsWorklogs as $worklog) {
            $month = $worklog->invoice_month;
            $categoryId = !empty($worklog->$categoryKey) ? $worklog->$categoryKey : -1;
            //user total logged value per month
            $userMonthValues = &$userValuesPerCategory[$month][$worklog->user_id];
            $userMonthValues['total'] ??= 0;
            $userMonthValues['total'] += $worklog->value;
            //user total logged value per category (project/owner/activity) per month
            $userMonthValues[$categoryId] ??= 0;
            $userMonthValues[$categoryId] += $worklog->value;
        }

        //calculate category expenses based on the users contribution to each category
        //and their total expenses for that month
        $categoryMonthCosts = [];
        foreach ($userValuesPerCategory as $month => $usersValues) {
            foreach ($usersValues as $userId => $userCategoryValues) {
                foreach ($userCategoryValues as $categoryId => $categoryValue) {
                    if ($categoryId == 'total') continue;
                    //get the percentage of the user log values of the category, relative to the total logged per month
                    $categoryRelativePercentage = 0;
                    if ($userCategoryValues['total']) {
                        $categoryRelativePercentage = $categoryValue / $userCategoryValues['total'];
                    }
                    //use the category logged ratio determine user costs associated with that category, from the user total monthly costs
                    $categoryMonthCosts[$month][$categoryId] ??= 0;
                    $categoryMonthCosts[$month][$categoryId] += ($this->usersCostsByMonth[$month][$userId] ?? 0) * $categoryRelativePercentage;
                }
            }
        }

        return $categoryMonthCosts;
    }

    /** Take into account the included value of subscription type project logs,
      * and eliminate that value from the user/activities income calculation */
    public function getSubscriptionsLogsValue(Collection $worklogs): void
    {
        $logsByFluxId = $worklogs->groupBy('flux_id');
        foreach ($logsByFluxId as $fluxId => $fluxLogs) {
            try {
                if ($this->billedFluxes[$fluxId]->payment_structure == PROJECT_PAYMENT_MONTHLY) {
                    $this->getSubscriptionsIncomeByFluxLogs($fluxLogs, $this->billedFluxes[$fluxId]);
                }
            } catch (\Throwable $e) {
                send_error_report('Workforce productivity - get logs value for fixed sum/subscriptions error', $e);
            }
        }
    }

    public function getUsersFixedSumProjectsValue(array &$userIncomeByMonth, ?Collection $worklogs = null, ?array &$usersIdsWithValues = null): void
    {
        $worklogs ??= $this->filteredWorklogs;
        $billedProjectsIds = collect($this->allInvoiceRowValues)->whereNotNull('project_id')->pluck('project_id');
        $billedProjectsIds = $billedProjectsIds->intersect($this->filters->projectsList)->toArray();
        $projectsWithLogs = $this->wpRepository->getFixedSumProjectsWithLogs($billedProjectsIds ?? null);

        //get the ratio of the total value users get as income from the projects
        $projectLoggedTimeRatio = $this->getFixedSumProjectLoggedTimeRatio($worklogs, $projectsWithLogs);

        //get the billed value per fixed sum project, per month, with the users
        $projectIncomePerMonth = [];
        foreach ($this->allInvoiceRowValues as $invoiceRow) {
            if (!isset($invoiceRow->project_id)) continue;
            $projectId = $invoiceRow->project_id;
            if (empty($projectsWithLogs[$projectId])) continue;

            $projectIncomePerMonth[$invoiceRow->invoice_month][$projectId] ??= 0;
            $projectIncomePerMonth[$invoiceRow->invoice_month][$projectId] += $invoiceRow->billed_value * ($projectLoggedTimeRatio[$projectId] ?? 1);
        }

        //get the total value grouped by project and the total value per project/user
        $usersLoggedTotal = [];
        $projectsLoggedValue = [];
        foreach ($worklogs as $log) {
            if (empty($projectsWithLogs[$log->project_id])) continue;

            $value = $log->included_in_value == 1 ? $log->value : ($log->rate * $log->billed_time / 60);

            $usersLoggedTotal[$log->project_id][$log->user_id] ??= 0;
            $usersLoggedTotal[$log->project_id][$log->user_id] += $value;
            $projectsLoggedValue[$log->project_id] ??= 0;
            $projectsLoggedValue[$log->project_id] += $value;
        }
        //use the two totals to calculate the ratio value per user, per project
        $usersIncomeRatio = [];
        foreach ($usersLoggedTotal as $projectId => $projectUsers) {
            foreach ($projectUsers as $userId => $userValue) {
                if (empty($projectsWithLogs[$projectId])) continue;

                if ($projectsLoggedValue[$projectId] != 0) {
                    $usersIncomeRatio[$projectId][$userId] = $userValue / $projectsLoggedValue[$projectId];
                } else {
                    //when the total logged values is 0, split the ratio equally between users
                    $usersIncomeRatio[$projectId][$userId] = 1 / count($projectUsers);
                }
            }
        }

        //use the user value ratio to assign each user income the proportional value per project
        foreach ($projectIncomePerMonth as $month => $monthBilledProjects) {
            foreach ($monthBilledProjects as $projectId => $projectMonthIncome) {
                if (!isset($usersIncomeRatio[$projectId])) continue;

                foreach ($usersIncomeRatio[$projectId] as $userId => $incomeRatio) {
                    $userIncomeByMonth[$month][$userId] ??= 0;
                    $userIncomeByMonth[$month][$userId] += $incomeRatio * $projectMonthIncome;
                    if (!is_null($usersIdsWithValues) && !in_array($userId, $usersIdsWithValues)) $usersIdsWithValues[] = $userId;
                }
            }
        }
    }

    /** used to calculate the value per user when there are no billed logs for the fixed sum project */
    public function getFixedSumNoLogsValue(array &$userIncomeByMonth, ?array &$usersIdsWithValues = null): void
    {
        $projectsWithoutLogs = $this->wpRepository->getFixedSumProjectsWithoutLogs($this->filters->projectsList ?? null);
        $projectMonthlyValues = [];

        foreach ($this->allInvoiceRowValues as $invoiceRow) {
            if (empty($projectsWithoutLogs[$invoiceRow->project_id]) || intval($invoiceRow->row_type) != INVOICE_FIXED_SUM) {
                continue;
            }
            $projectMonthlyValues[$invoiceRow->invoice_month][$invoiceRow->project_id] ??= 0;
            $projectMonthlyValues[$invoiceRow->invoice_month][$invoiceRow->project_id] += $invoiceRow->billed_value;
        }

        foreach ($projectMonthlyValues as $month => $projectListValues) {
            foreach ($projectListValues as $projectId => $projectBilledValue) {
                $projectWorkingUsers = $projectsWithoutLogs[$projectId];
                $totalHourlyRate = 0;
                foreach ($projectWorkingUsers as $userData) {
                    //convert the users hourly rate
                    if (isset($userData->currency)) {
                        $userData->rate *= exchange_currency($this->defaultCurrency, $userData->currency);
                        unset($userData->currency);
                    }
                    //get the total hourly rate, to use to express each user's proportion
                    $totalHourlyRate += $userData->rate;
                }
                //add the proportional uses value to the total users income object
                $userValueRatio = 1 / count($projectWorkingUsers);
                foreach ($projectWorkingUsers as $userData) {
                    $userIncomeByMonth[$month][$userData->user_id] ??= 0;
                    if ($totalHourlyRate > 0) $userValueRatio = $userData->rate / $totalHourlyRate;
                    $userIncomeByMonth[$month][$userData->user_id] += $userValueRatio * $projectBilledValue;
                    if (!is_null($usersIdsWithValues) && !in_array($userData->user_id, $usersIdsWithValues)) {
                        $usersIdsWithValues[] = $userData->user_id;
                    }
                }
            }
        }
    }

    public function getUserCosts(array $userIncomeByMonth): array
    {
        //calculate the user income for all projects, which we will use to determine the cost ratio per project
        $this->getSubscriptionsLogsValue($this->allProjectsWorklogs);
        $userIncomeByMonthAllProjects = $this->getMonthlyIncomeByFluxLogs(
            worklogs: $this->allProjectsWorklogs,
            categoryKey: 'user_id',
            filteredCategoryIds: $this->filters->usersList
        );
        $this->getUsersFixedSumProjectsValue($userIncomeByMonthAllProjects, $this->allProjectsWorklogs);
        $this->getFixedSumNoLogsValue($userIncomeByMonthAllProjects);

        //calculate the total costs by month with the projects filter applied, using the total and filtered user value per project
        $costRatio = [];
        $costsByMonth = [];
        foreach ($this->usersCostsByMonth as $month => $usersMonthCosts) {
            foreach ($usersMonthCosts as $userId => $userCosts) {
                $costRatio[$month][$userId] = 0;
                //if the user has no income in any projects, then show the complete cost regardless of the project filters
                //since it means that the costs are unrelated to any project
                if (empty($userIncomeByMonthAllProjects[$month][$userId])) {
                    $costRatio[$month][$userId] = 1;
                //user has costs per month related to the projects, so apply a custom ratio
                } else if (($userIncomeByMonth[$month][$userId] ?? 0) > 0) {
                    $costRatio[$month][$userId] = $userIncomeByMonth[$month][$userId] / $userIncomeByMonthAllProjects[$month][$userId];
                }
                $costsByMonth[$month][$userId] = $userCosts * $costRatio[$month][$userId];
            }
        }

        return $costsByMonth;
    }

    //===============================================================================================================================
    // Private
    //===============================================================================================================================

    private function getSubscriptionsIncomeByFluxLogs(Collection $fluxWorklogs, object $fluxInfo): void
    {
        $projectTimeExceeded = false;
        $minutesValidated = 0;
        //get the value ratio between the users logs initial value and the actual billed value, to apply later
        $billedValueRateRatio = $this->getLogsInitialToBilledValueRatio($fluxWorklogs, $fluxInfo);

        foreach ($fluxWorklogs as $log) {
            if ($log->billing_type != WORKLOG_TYPE_BILLABLE) continue;

            $minutesValidated += ($log->billable ?? $log->billed_time);
            //the value any time that is not over the project time in calculated at the special project hourly rate
            if ($minutesValidated <= $fluxInfo->project_hours * HOUR_MINUTES) {
                //express the user hourly rate relative to the actual billed value
                $log->rate *= $billedValueRateRatio;
                $log->value = get_log_value($log, $this->defaultCurrency);
                continue;
            }
            //if the time is over the project hours, then it is calculated at the user's own hourly rate
            $logValue = get_log_value($log, $this->defaultCurrency);
            $log->value = $logValue;
            //when the time is first exceeded, calculate the split time log value at the two different rates
            if (!$projectTimeExceeded) {
                $projectTimeExceeded = true;
                //when custom value exists ignore extra value calculation
                if (!isset($log->custom_value)) {
                    $splitOverTime = $minutesValidated - ($fluxInfo->project_hours * HOUR_MINUTES);
                    $initialRateTime = $log->billed_time - $splitOverTime;

                    $logFirstHalfValue = $log->rate * $billedValueRateRatio * $initialRateTime / HOUR_MINUTES;
                    $logSecondHalfValue = $log->rate * $splitOverTime / HOUR_MINUTES;
                    $log->value = ($logFirstHalfValue + $logSecondHalfValue) * $log->exchange_rate;
                }
            }
        }
    }

    private function getLogsInitialToBilledValueRatio(Collection $worklogs, object $fluxInfo): float|int
    {
        if ($fluxInfo->project_hours == 0) return 1;

        //special hourly rate for the hours included in the monthly billed projects
        $projectBilledIncludedValue = $fluxInfo->billed_value - ($fluxInfo->billed_extra_value ?? 0);
        $projectAverageHourlyRate = $projectBilledIncludedValue / $fluxInfo->project_hours;
        //get the bill value hourly rate multiplier
        $totalLogsValue = 0;
        $totalLogsTime = 0;
        foreach ($worklogs as $log) {
            //calculate the total time and value for the project included time, at the user's own rate
            if ($totalLogsTime + $log->billed_time <= $fluxInfo->project_hours * HOUR_MINUTES) {
                $totalLogsTime += $log->billed_time;
                $totalLogsValue += get_log_value($log, $this->defaultCurrency);
            } else {
                //extra case only one time if a log partly exceeds the included value
                $remainingBillableTime = $fluxInfo->project_hours * HOUR_MINUTES - $totalLogsTime;
                if ($remainingBillableTime > 0) {
                    $splitLog = clone $log;
                    $totalLogsTime += $remainingBillableTime;
                    $splitLog->billed_time = $remainingBillableTime;
                    $totalLogsValue += get_log_value($splitLog, $this->defaultCurrency);
                }
                break;
            }
        }
        if ($totalLogsTime == 0 || $totalLogsValue == 0) return 0;

        return $projectAverageHourlyRate * ($totalLogsTime / HOUR_MINUTES) / $totalLogsValue;
    }

    private function getFixedSumProjectLoggedTimeRatio($worklogs, $projectsWithLogs): array
    {
        $projectsLoggedTime = [];
        //get the total logged time
        foreach ($worklogs as $log) {
            if (empty($projectsWithLogs[$log->project_id])) continue;

            $projectsLoggedTime[$log->project_id] ??= 0;
            $projectsLoggedTime[$log->project_id] += $log->billed_time;
        }

        $projectHoursRatio = [];
        foreach ($projectsLoggedTime as $projectId => $projectLoggedTime) {
            $projectInfo = $projectsWithLogs[$projectId];
            //ignore the calculation for specific cases
            if ($projectInfo->project_hours == 0 || $projectLoggedTime == 0 ||
               ($projectInfo->include_extra_hours == 1 && $projectInfo->logs_relative_value == 0)
            ) {
                $projectHoursRatio[$projectId] = 1;
                continue;
            }
            //ignore the cost ratio in every case other than $projectInfo->include_extra_hours == 1 && $loggedTime < $projectInfo->project_hours
            $projectHoursRatio[$projectId] = ($projectLoggedTime / HOUR_MINUTES) / $projectInfo->project_hours;
            //the ratio can never be higher than 1
            if ($projectHoursRatio[$projectId] > 1) $projectHoursRatio[$projectId] = 1;
        }

        return $projectHoursRatio;
    }

    private function getUsersCostsGroupedByMonth(): array
    {
        $costPerMonth = [];
        $usersAndTeamsCosts = $this->wpRepository->getUsersAndTeamsCosts($this->filters);
        $usersGroupedByTeams = $this->wpRepository->getUsersGroupedByTeams();

        foreach ($usersAndTeamsCosts as $cost) {
            //if it's a team cost, then split the cost equally among the team users
            if (!empty($cost->team_id)) {
                $teamUsersCount = count($usersGroupedByTeams[$cost->team_id]);
                if ($teamUsersCount == 0) continue;
                foreach ($usersGroupedByTeams[$cost->team_id] as $teamUserId) {
                    $costPerMonth[$cost->cost_month][$teamUserId] ??= 0;
                    $costPerMonth[$cost->cost_month][$teamUserId] += $cost->value / $teamUsersCount;
                }
            } else {
                $costPerMonth[$cost->cost_month][$cost->user_id] ??= 0;
                $costPerMonth[$cost->cost_month][$cost->user_id] += $cost->value;
            }
        }

        return $costPerMonth;
    }
}
