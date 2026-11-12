local floor = math.floor
local reading, limit = reading, limit

function Price(scenario)
    local total = 0.0
    local discount = 1.0 + (40 + scenario) / 10000.0
    for loan = 0, 31 do
        local balance = 80000.0 + loan * 1250
        local principal = balance / 120.0
        local coupon = (35 + loan % 11) / 10000.0
        local factor = 1.0
        for month = 1, 120 do
            factor = factor / discount
            total = total + (principal + balance * coupon) * factor
            balance = balance - principal
        end
    end
    return floor(total * 100.0)
end

function Scan(batch)
    local total = 0
    local base = batch * 31
    for i = base, base + 127 do
        local value = reading(i)
        local threshold = limit(i)
        if value > threshold then
            total = total + value - threshold
        end
    end
    return total
end

local function fibonacci(n)
    if n < 2 then return n end
    return fibonacci(n-2) + fibonacci(n-1)
end

function Fib(seed)
    return fibonacci(14 + seed)
end

function Sieve(seed)
    local n = 5000 + 31*seed
    local flags = {}
    for i = 1, n do
        flags[i] = true
    end
    local count = 0
    for i = 2, n do
        if flags[i] then
            count = count + 1
            local k = i+i
            while k <= n do
                flags[k] = false
                k = k+i
            end
        end
    end
    return count
end

function Mandelbrot(seed)
    local n = 24 + 2*seed
    local scale = 2.0/n
    local inside = 0
    for y = 0, n-1 do
        local ci = y*scale - 1.0
        for x = 0, n-1 do
            local cr = x*scale - 1.5
            local zr, zi = cr, ci
            local rr, ii = zr*zr, zi*zi
            inside = inside + 1
            for i = 1, 49 do
                zi = zr*zi*2.0 + ci
                zr = rr-ii + cr
                ii, rr = zi*zi, zr*zr
                if rr+ii > 4.0 then
                    inside = inside - 1
                    break
                end
            end
        end
    end
    return inside
end
