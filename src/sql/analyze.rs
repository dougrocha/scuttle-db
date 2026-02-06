use crate::{
    Schema,
    sql::{
        logical_planner::LogicalPlan,
        parser::{Expression, Statement, TargetEntry, TargetList},
        planner_context::PlannerContext,
    },
};
use miette::{Result, miette};

pub struct Analyzer<'a> {
    context: &'a PlannerContext<'a>,
}

impl<'a> Analyzer<'a> {
    pub fn new(context: &'a PlannerContext) -> Self {
        Self { context }
    }

    pub fn analyze_plan(&mut self, statement: Statement) -> Result<LogicalPlan> {
        match statement {
            Statement::Select {
                table,
                targets,
                r#where,
            } => {
                let resolved_table = self.context.get_table(&table)?;

                let (expanded_targets, names) =
                    self.expand_targets(targets, &resolved_table.schema)?;

                let mut plan = LogicalPlan::TableScan {
                    table: resolved_table.name.clone(),
                };

                if let Some(predicate) = r#where {
                    plan = LogicalPlan::Filter {
                        predicate,
                        input: Box::new(plan),
                    };
                }

                Ok(LogicalPlan::Projection {
                    expressions: expanded_targets,
                    column_names: names,
                    input: Box::new(plan),
                })
            }
            Statement::Create => Err(miette!("CREATE not implemented yet")),
            Statement::Update { .. } => Err(miette!("INSERT not implemented yet")),
            Statement::Insert => Err(miette!("INSERT not implemented yet")),
            Statement::Delete => Err(miette!("DELETE not implemented yet")),
        }
    }

    fn expand_targets(
        &self,
        targets: TargetList,
        schema: &Schema,
    ) -> Result<(Vec<Expression>, Vec<String>)> {
        let mut expanded = Vec::new();
        let mut names = Vec::new();

        for target in targets {
            match target {
                TargetEntry::Star => {
                    // This is the "Star Expansion" logic
                    for col in &schema.columns {
                        expanded.push(Expression::Column(col.name.clone()));
                        names.push(col.name.to_string());
                    }
                }
                TargetEntry::Expression { expr, alias } => {
                    expanded.push(expr.clone());
                    names.push(alias.clone().unwrap_or_else(|| expr.to_column_name()));
                }
            }
        }

        Ok((expanded, names))
    }
}
