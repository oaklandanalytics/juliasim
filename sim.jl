using DataFrames, GLM, JLD

# FIXME - shouldn't be here
cd("$(homedir())/src/julia")


##############
### TABLES ###
##############


function buildings()
  b = jld()["buildings"][[:building_id, :parcel_id, :residential_units,
                          :non_residential_sqft, :building_sqft,
                          :stories, :year_built, :redfin_sale_price,
                          :redfin_sale_year, :costar_rent,
                          :building_type_id]]
  # parking lots sometimes get building sqft, which they shouldn't
  b[findin(b[:building_type_id], [15, 16]), :building_sqft] = 0
  b
end


function households()
  h = jld()["households"][[:household_id, :income, :persons, :tenure,
                           :hworkers, :building_id]]
  h[:income_quartile] = 1
  breaks = quantile(h[:income])
  # FIXME qcut in julia?
  for q in 2:4
    h[breaks[q] .< h[:income] .<= breaks[q+1], :income_quartile] = q
  end
  h
end


function parcels()
  # shape_area is in meters
  jld()["parcels.csv"][[:parcel_id, :zone_id, :county_id, :geom_id,
                        :proportion_undevelopable, :x, :y, :shape_area]]
end


#############
### JOINS ###
#############


function parcels_zones(P, Z)
  join(P, Z, on = :zone_id)
end


function buildings_parcels(B, P)
  join(B, P, on = :parcel_id)
end


function households_buildings(H, B)
  join(H, B, on = :building_id)
end


function jobs_buildings(J, B)
  join(J, B, on = :building_id)
end


#################
### VARIABLES ###
#################


function zone_aggregations(H, B, P)
  B_P = buildings_parcels(B, P)
  H_B = households_buildings(H, B_P)

  # aggregations on buildings and parcels
  Z1 = by(B_P, :zone_id) do df
    DataFrame(
      total_residential_units = sum(df[:residential_units]),
      total_non_residential_sqft = sum(df[:non_residential_sqft])
    )
  end

  # aggregations on households
  Z2 = by(H_B, :zone_id) do df
    DataFrame(
      total_population = sum(df[:persons]),
      average_income = median(df[:income])
    )
  end

  join(Z1, Z2, on = :zone_id)
end


##############
### MODELS ###
##############


# FIXME how to do expressions in the formula?
rsh_formula = redfin_sale_price ~ building_sqft + total_residential_units +
  average_income
rsh_OLS = None


# residential sales price hedonic estimation
function rsh_estimate(df)
  df = df[~isna(df[:redfin_sale_price]), :]  # drop nas in the outcome variable
  df = df[df[:redfin_sale_year] .> 2010, :]
  df = df[df[:redfin_sale_year] .<= 2014, :]
  rsh_OLS = glm(rsh_formula, df, Normal(), IdentityLink())
end


function rsh_simulate(df)
  hh_df[:residential_sales_price] = predict(rsh_OLS, df)
end


function household_transition(hh_df, year)

  hc = readtable("household_controls.csv")
  # get the row for this year in the controls
  row = hc[hc[:year] .== year, :]

  for quartile in 1:4

    # get the target
    target = row[symbol(string("q", quartile, "_households"))][1]

    # current quartile
    indexes = find(hh_df[:income_quartile] .== quartile)

    target -= size(indexes, 1)
    
    println("Adding $target households for quartile $quartile")

    if target > 0
      add_df = hh_df[sample(indexes, target), :]
      hh_df = vcat(hh_df, add_df)
    elseif target < 0
      hh_df = deleterows(hh_df, sample(indexes, target*-1, replace=false))
    end

  end

  println("Countmap by income quartile:")
  println(countmap(hh_df[:income_quartile]))
  
  hh_df
end


function feasibility(P, P_B)
  # TODO get total_current_sqft for buildings on the parcel
  for i in 1:size(P, 1)
    p = P[i]
    revenue = residential_sqft_proforma(
      p[:parcel_acres], current_sqft, allowed_dua,
                                   purchase_price_sqft, sales_price_sqft,
                                   average_unit_sqft, building_efficiency,
                                   construction_cost
    )
  end
end


###############
### MODULES ###
###############


function residential_sqft_proforma(parcel_acres, current_sqft, allowed_dua,
                                   purchase_price_sqft, sales_price_sqft,
                                   average_unit_sqft, building_efficiency,
                                   construction_cost)
  sqft = parcel_acres * allowed_dua * average_unit_sqft
  total_revenue = sqft * sales_price_sqft * building_efficiency
  construction_cost = sqft * construction_cost
  purchase_price = current_sqft * purchase_price_sqft
  return total_revenue - construction_cost - purchase_price
end


#################
### UTILITIES ###
#################


function len(df::DataFrame)
  return size(df, 1)
end


function deleterows(df::DataFrame, rows::Array{Int,1})
  return df[setdiff(1:end,rows), :]
end


# this goes from csvs to binary for quick i/o
function csvs_to_jld()
  jldopen("bayarea_v3.jld", "w") do file
    write(file, "parcels", readtable("parcels.csv"))
    write(file, "buildings", readtable("buildings.csv"))
    write(file, "households", readtable("households.csv"))
    write(file, "jobs", readtable("jobs.csv"))
    write(file, "zones", readtable("zones.csv"))
  end
end


function jld()
  # only open the cache once, return it if already opened
  haskey(CACHE, "jld") || (CACHE["jld"] = load("bayarea_v3.jld"))
  CACHE["jld"]
end

CACHE = Dict()


# the last line gets returned
"HAPPY JULIASIM-ING!"
