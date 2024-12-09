# This script test the "completion rate" of archived data
using Format
using Test

datadir = "/g/data/xv83/TMIP/data"
modeldir = joinpath(datadir, "ACCESS-ESM1-5")
experiments = ["historical", "ssp370"]
exp_prefix = ["HI", "SSP-370"]
vars = ["$(str1)_trans_$(str2)" for str1 in ("tx", "ty") for str2 in ("gm", "submeso")]

@testset "archived data" begin
    @testset "$(member)" for member in 1:40
        @testset "$(experiment)" for (experiment, exp_prefix) in zip(experiments, exp_prefix)
            expdir = joinpath(modeldir, experiment)
            CSIRO_member = "$exp_prefix-$(format(member + 4, width=2, zeropadding=true))"
            memberdir = joinpath(expdir, CSIRO_member)
            decades = (experiment == "historical") ? (1850:10:2010) : (2010:10:2090)
            @test all(isfile(joinpath(memberdir, "month_$(var)_$(decade)s.nc")) for var in vars for decade in decades)
        end
    end
end
