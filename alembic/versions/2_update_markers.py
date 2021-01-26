"""Update markers

Revision ID: 2
Revises: 1
Create Date: 2021-01-26 15:55:10.120351

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '2'
down_revision = '1'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('cells', sa.Column('perk_t202_y204_1__cell_masks', sa.Numeric(precision=15, scale=4), nullable=True))
    op.add_column('cells', sa.Column('perk_t202_y204_1__nuclei_masks', sa.Numeric(precision=15, scale=4), nullable=True))
    op.add_column('cells', sa.Column('perk_t202_y204_2__cell_masks', sa.Numeric(precision=15, scale=4), nullable=True))
    op.add_column('cells', sa.Column('perk_t202_y204_2__nuclei_masks', sa.Numeric(precision=15, scale=4), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('cells', 'perk_t202_y204_2__nuclei_masks')
    op.drop_column('cells', 'perk_t202_y204_2__cell_masks')
    op.drop_column('cells', 'perk_t202_y204_1__nuclei_masks')
    op.drop_column('cells', 'perk_t202_y204_1__cell_masks')
    # ### end Alembic commands ###
