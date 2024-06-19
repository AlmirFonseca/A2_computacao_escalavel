-- Create a script to update the price of 1000 random products, to a random value between 1 and 1000 (for more or less, like -1, +80, -100, etc)
-- The script should be idempotent, meaning that it should be able to run multiple times without causing any issues

-- Create the function to update prices
CREATE OR REPLACE FUNCTION update_random_product_prices() RETURNS VOID AS $$
DECLARE
    product_id INTEGER;
    current_price DECIMAL(10, 2);
    new_price DECIMAL(10, 2);
BEGIN
    -- Select 1000 random product IDs
    FOR product_id IN
        SELECT id_product
        FROM conta_verde.products
        ORDER BY random()
        LIMIT 1
    LOOP
        -- Get the current price
        SELECT price INTO current_price FROM conta_verde.products WHERE id_product = product_id;

        -- Generate a random adjustment between -100 and +1000
        new_price := current_price + round((random() * 1100 - 100)::numeric, 2);

        -- Ensure the new price is at least 1
        IF new_price < 1 THEN
            new_price := 1;
        END IF;

        -- Update the product price
        UPDATE conta_verde.products
        SET price = new_price
        WHERE id_product = product_id;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Execute the function to update prices
SELECT update_random_product_prices();
