# This script test the "completion rate" of making averaged variables
using Format

datadir = "/scratch/xv83/TMIP/data"
modeldir = joinpath(datadir, "ACCESS-ESM1-5")
decades = [1850, 2030, 2090]
# decades = [1990]
experiments = [t < 2010 ? "historical" : "ssp370" for t in decades]
vars = ("umo", "vmo", "mlotst", "tx_trans_gm", "ty_trans_gm", "tx_trans_submeso", "ty_trans_submeso")
members = map(m -> "r$(m)i1p1f1", 1:40)

using Test
files = ["$var.nc" for var in vars]
@testset "saved data" begin
    @testset "$(decade) $(experiment)" for (decade, experiment) in zip(decades, experiments)
        experimentdir = joinpath(modeldir, experiment)
        @testset "$(member)" for member in members
            dir = joinpath(experimentdir, member, "Jan$(decade)-Dec$(decade + 9)", "cyclomonth")
            # @test all(isfile(joinpath(dir, file)) for file in files)
            @testset "$(var)" for var in vars
                @test isfile(joinpath(dir, "$var.nc"))
            end
        end
    end
end